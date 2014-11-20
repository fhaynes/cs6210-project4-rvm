#include "rvm.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include "seqsrchst.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <errno.h>
/*
  Initialize the library with the specified directory as backing store.
*/

// Global variables
// This is our global redolog
redo_t redo_log;

// Hash table for segbase<->segment_t
seqsrchst_t ss;

// A queue for transaction IDs
steque_t t_q;

// Constants
int my_pid;
int num_transactions = 100;
char redo_file_path[512];

int equals(seqsrchst_key a, seqsrchst_key b) {
	int x, y;
	x = *(int *) a;
	y = *(int *) b;
	if (x == y) {
		return 1;
	} else {
		return 0;
	}
}

rvm_t rvm_init(const char *directory){
	my_pid = getpid();
	printf("DEBUG %d: Beginning rvm_init\n", my_pid);
	printf("DEBUG %d: Initializing hash table\n", my_pid);
	seqsrchst_init(&ss, equals);
	rvm_t rvm;
	rvm = malloc(sizeof(struct _rvm_t));
	strcpy(rvm->prefix, directory);
	printf("DEBUG %d: rvm->prefix is %s\n", my_pid, rvm->prefix);
	// Let's create the directory to hold the log and segment files
	mkdir(directory, 0777);
	// Allocate the redo log
	redo_log = malloc(sizeof(struct _redo_t));
	redo_log->numentries = 0;
	printf("DEBUG %d: Allocating entries in redo log struct\n", my_pid);
	redo_log->entries = malloc(sizeof(segentry_t) * num_transactions);

	// Need to build the redo file path
	strcpy(redo_file_path, rvm->prefix);
	strcat(redo_file_path, "/");
	strcat(redo_file_path, "rvm.log");

	// Now let's make sure the redo file exists
	int r_fd;
	r_fd = open(redo_file_path, O_RDONLY | O_CREAT, S_IRWXU);
	if (r_fd == -1) {
		printf("DEBUG %d: There was a problem opening or creating the redo file log\n", my_pid);
		exit(1);
	}
	// Close the file as we don't need it right now
	close(r_fd);
	printf("DEBUG %d: Done with rvm_init\n", my_pid);
	return rvm;

}

/*
  map a segment from disk into memory. If the segment does not already exist, then create it and give it size size_to_create. If the segment exists but is shorter than size_to_create, then extend it until it is long enough. It is an error to try to map the same segment twice.
*/
void *rvm_map(rvm_t rvm, const char *segname, int size_to_create){
	printf("DEBUG %d: Beginning rvm_map\n", my_pid);
	rvm_truncate_log(rvm);
	// Need a place to hold the seg file path
	char seg_file_path[512];
	strcpy(seg_file_path, rvm->prefix);
	strcat(seg_file_path, "/");
	strcat(seg_file_path, segname);

	// Let's open that file!
	printf("DEBUG %d: Opening %s for segment file\n", my_pid, seg_file_path);
	int seg_file;
	int result;
	seg_file = open(seg_file_path, O_RDWR | O_CREAT, S_IRWXU);
	if (seg_file == -1) {
		printf("DEBUG %d: Error opening seg_descriptor file %s\n", my_pid, seg_file_path);
		exit(1);
	}

	// Now we need to check file size and see if it is large enough
	struct stat st;
	stat(seg_file_path, &st);
	int size_diff;
	size_diff = size_to_create - st.st_size;
	// If there is a difference, we gotta expand the file
	if (size_diff > 0) {
		printf("-DEBUG %d: Truncating segment file, needed %d more\n", my_pid, size_diff);
		int trun_res;
		trun_res = truncate(seg_file_path, size_diff);
		if (trun_res == -1) {
			printf("--DEBUG %d: Error truncating file\n", my_pid);
		}
	}

	// Now that we have a blank file, we need to create a segment struct to represent it
	segment_t seg;
	// Malloc the seg struct first
	printf("DEBUG %d: Malloc'ing segment_t struct\n", my_pid);
	seg = malloc(sizeof(struct _segment_t));
	// Now the area of memory that is going to map to what is on disk
	seg->segbase = malloc(size_to_create);
	printf("DEBUG %d: Done malloc'ing segbase\n", my_pid);
	strcpy(seg->segname, segname);
	printf("DEBUG %d: Done strcpy to segname\n", my_pid);
	seg->size = size_to_create;
	printf("DEBUG %d: Done assigning size_to_create\n", my_pid);
	steque_init(&seg->mods);
	printf("DEBUG %d: Done initing steque\n", my_pid);
	//rewind(seg_file);
	result = pread(seg_file, seg->segbase, size_to_create, 0);
	printf("DEBUG %d: Result of fread to segbase was %d\n", my_pid, result);
	close(seg_file);
	seqsrchst_put(&ss, seg->segbase, seg);
	printf("DEBUG %d: Done with rvm_map\n", my_pid);
	return seg->segbase;
}

/*
  unmap a segment from memory.
*/
void rvm_unmap(rvm_t rvm, void *segbase){
	printf("DEBUG %d: Beginning unmap\n", my_pid);
	segment_t seg_to_unmap;
	seg_to_unmap = (segment_t) seqsrchst_get(&ss, segbase);
	seqsrchst_delete(&ss, segbase);
	free(seg_to_unmap);
	printf("DEBUG %d: Done unmap\n", my_pid);

}

/*
  destroy a segment completely, erasing its backing store. This function should not be called on a segment that is currently mapped.
 */
void rvm_destroy(rvm_t rvm, const char *segname){
	printf("DEBUG %d: Beginning rvm_destroy\n", my_pid);
	char file_to_destroy[512];
	strcpy(file_to_destroy, rvm->prefix);
	strcat(file_to_destroy, "/");
	strcat(file_to_destroy, segname);
	remove(file_to_destroy);
}

/*
  begin a transaction that will modify the segments listed in segbases. If any of the specified segments is already being modified by a transaction, then the call should fail and return (trans_t) -1. Note that trant_t needs to be able to be typecasted to an integer type.
 */
trans_t rvm_begin_trans(rvm_t rvm, int numsegs, void **segbases){
	printf("DEBUG %d: Begin rvm_begin_trans\n", my_pid);
	// Let's allocate a trans_t struct to hold our stuff
	trans_t t;
	t = malloc(sizeof(struct _trans_t));
	t->rvm = rvm;
	t->numsegs = numsegs;
	// We need to have an array of pointers to segments
	t->segments = malloc(sizeof(segment_t) * numsegs);
	int i, j;
	// So this loop iterates over each segment we are passed in and assigns it to the transaction
	for (i = 0; i < numsegs; i++) {
		for (j = 0; j < seqsrchst_size(&ss); j++) {
			t->segments[i] = (segment_t) seqsrchst_get(&ss, segbases[i]);
			if (t->segments[i]->cur_trans != NULL) {
				return (trans_t) -1;
			}
			t->segments[i]->cur_trans = t;
			printf("-DEBUG %d: Assigned %s current transaction\n", my_pid, t->segments[i]->segname);
		}
	}
	printf("DEBUG %d: Done with rvm_begin_trans\n", my_pid);
	return t;
}

/*
  declare that the library is about to modify a specified range of memory in the specified segment. The segment must be one of the segments specified in the call to rvm_begin_trans. Your library needs to ensure that the old memory has been saved, in case an abort is executed. It is legal call rvm_about_to_modify multiple times on the same memory area.
*/
void rvm_about_to_modify(trans_t tid, void *segbase, int offset, int size){
	printf("DEBUG %d: Beginning rvm_about_to_modify\n", my_pid);
	segment_t tar_seg;
	tar_seg = (segment_t) seqsrchst_get(&ss, segbase);
	printf("DEBUG %d: Found tar_seg with name %s\n", my_pid, tar_seg->segname);
	printf("DEBUG %d: Creating mod_t for transaction\n", my_pid);
	mod_t* n;
	n = malloc(sizeof(mod_t));
	n->undo = malloc(size);
	n->offset = offset;
	n->size = size;
	printf("DEBUG %d: mod_t created with offset %d and size %d\n", my_pid, offset, size);
	memcpy(n->undo, segbase+offset, size);
	steque_enqueue(&tar_seg->mods, n);
	printf("DEBUG %d: Done with rvm_about_to_modify\n", my_pid);
}

/*
commit all changes that have been made within the specified transaction. When the call returns, then enough information should have been saved to disk so that, even if the program crashes, the changes will be seen by the program when it restarts.
*/
void rvm_commit_trans(trans_t tid){
	printf("DEBUG %d: Begin rvm_commit_trans\n", my_pid);
	int i;
	// This loop iterates through each segment in a transaction and applies the changes
	for (i = 0; i < tid->numsegs; i++) {
		printf("-DEBUG %d: Processing segment %s of transaction\n", my_pid, tid->segments[i]->segname);
		int trans_id;
		trans_id = redo_log->numentries;
		strcpy(redo_log->entries[trans_id].segname, tid->segments[i]->segname);
		redo_log->entries[trans_id].segsize = tid->segments[i]->size;
		redo_log->entries[trans_id].numupdates = steque_size(&tid->segments[i]->mods);
		redo_log->entries[trans_id].data = calloc(tid->segments[i]->size, 1);
		redo_log->entries[trans_id].offsets = malloc(sizeof(int) * redo_log->entries[trans_id].numupdates);
		redo_log->entries[trans_id].sizes = malloc(sizeof(int) * redo_log->entries[trans_id].numupdates);
		int c = 0;
		while (steque_size(&tid->segments[i]->mods) > 0) {
			printf("--DEBUG %d: Processing modification\n", my_pid);
			mod_t* temp_mod;
			temp_mod = (mod_t *) steque_pop(&tid->segments[i]->mods);
			redo_log->entries[trans_id].offsets[c] = temp_mod->offset;
			redo_log->entries[trans_id].sizes[c] = temp_mod->size;
			memcpy(redo_log->entries[trans_id].data+temp_mod->offset, tid->segments[i]->segbase+temp_mod->offset, temp_mod->size);
			c++;
			printf("--DEBUG %d: String written to memory was %s\n", my_pid, (char *) redo_log->entries[trans_id].data);
			free(temp_mod);
		}
		redo_log->numentries++;
		printf("DEBUG %d: Done processing modifications of each segment in transaction\n", my_pid);
		printf("DEBUG %d: Begin writing redo_file to disk\n", my_pid);
		printf("-DEBUG %d: Writing entry %d\n", my_pid, i);
		int redo_fh, offset;
		redo_fh = open(redo_file_path, O_WRONLY);
		if (redo_fh == -1) {
			printf("--DEBUG %d: There was an error opening the redo file!\n", my_pid);
			printf("--DEBUG %d: Error was %s\n", my_pid, strerror(errno));
			exit(1);
		}
		printf("-DEBUG %d: redo_fh open result was %d\n", my_pid, redo_fh);
		offset = lseek(redo_fh, 0, SEEK_END);
		printf("--DEBUG %d: Offset is %d\n", my_pid, offset);
		int result;
		result = pwrite(redo_fh, redo_log->entries[trans_id].segname, strlen(redo_log->entries[trans_id].segname), offset);
		offset += 128;
		printf("--D$EBUG %d: Wrote segname with result of %d\n", my_pid, result);
		if (result == -1) {
			printf("---DEBUG %d: Errno was %d\n", my_pid, strerror(errno));
			perror("pwrite()");
			exit(1);
		}
		pwrite(redo_fh, &redo_log->entries[trans_id].segsize, sizeof(int), offset);
		offset += sizeof(int);
		printf("--DEBUG %d: Wrote segsize\n", my_pid);
		pwrite(redo_fh, &redo_log->entries[trans_id].numupdates, sizeof(int), offset);
		offset += sizeof(int);
		printf("--DEBUG %d: Wrote numupdates\n", my_pid);
		int j;
		for (j = 0; j < redo_log->entries[trans_id].numupdates; j++) {
			printf("---DEBUG %d: Processing size entry %d/%d\n", my_pid, j, redo_log->entries[trans_id].numupdates);
			printf("---DEBUG %d: Size entry is %d\n", my_pid, redo_log->entries[trans_id].sizes[j]);
			pwrite(redo_fh, &redo_log->entries[trans_id].sizes[j], sizeof(int), offset);
			offset += sizeof(int);
		}
		printf("--DEBUG %d: Write sizes\n", my_pid);
		for (j = 0; j < redo_log->entries[trans_id].numupdates; j++) {
			pwrite(redo_fh, &redo_log->entries[trans_id].offsets[j], sizeof(int), offset);
			offset += sizeof(int);
		}
		printf("--DEBUG %d: Writing data of size %d\n", my_pid, redo_log->entries[trans_id].segsize);
		pwrite(redo_fh, redo_log->entries[trans_id].data, redo_log->entries[trans_id].segsize, offset);
		offset += redo_log->entries[trans_id].segsize;
		printf("--DEBUG %d: Void ptr to data shows %s\n", my_pid, (char *) redo_log->entries[trans_id].data);
		close(redo_fh);
	}
}

/*
  undo all changes that have happened within the specified transaction.
 */
void rvm_abort_trans(trans_t tid){


}

/*
 play through any committed or aborted items in the log file(s) and shrink the log file(s) as much as possible.
*/
void rvm_truncate_log(rvm_t rvm){
	printf("DEBUG %d: Begin rvm_truncate_log\n", my_pid);
	int redo_fh, offset;
	redo_fh = open(redo_file_path, O_RDONLY);
	if (redo_fh == -1) {
		printf("DEBUG %d: Error opening redo file!\n", my_pid);
		exit(1);
	}
	offset = lseek(redo_fh, 0, SEEK_CUR);
	printf("DEBUG %d: Result of lseek in truncate log is %d\n", my_pid, offset);
	int more = 1;
	printf("DEBUG %d: Beginning reading of segments from log file\n", my_pid);
	while (more == 1) {
		segentry_t *s;
		s = malloc(sizeof(segentry_t));
		int read_result;
		read_result = pread(redo_fh, &s->segname, 128, offset);
		printf("DEBUG %d: read_result in truncate log is %d\n", my_pid, read_result);
		if (read_result == 0) {
			close(redo_fh);
			printf("-DEBUG %d: EOF in redo log reached!\n", my_pid);
			remove(redo_file_path);
			redo_fh = open(redo_file_path, O_WRONLY | O_CREAT, S_IRWXU);
			close(redo_fh);
			more = 0;
			break;
		}
		offset += 128;
		pread(redo_fh, &s->segsize, sizeof(int), offset);
		offset += sizeof(int);
		//read(&s->numupdates, sizeof(int), 1, redo_fh);
		pread(redo_fh, &s->numupdates, sizeof(int), offset);
		offset += sizeof(int);
		printf("DEBUG %d: Read in segsize %d and numupdates %d\n", my_pid, s->segsize, s->numupdates);
		s->offsets = malloc(sizeof(int) * s->numupdates);
		s->sizes = malloc(sizeof(int) * s->numupdates);
		printf("DEBUG %d: Done allocating offsets and sizes\n", my_pid);
		int i;
		for (i = 0; i < s->numupdates; i++) {
			pread(redo_fh, &s->sizes[i], sizeof(int), offset);
			offset += sizeof(int);
		}
		for (i = 0; i < s->numupdates; i++) {
			pread(redo_fh, &s->offsets[i], sizeof(int), offset);
			offset += sizeof(int);
		}
		printf("DEBUG %d: Done reading in offsets and sizes\n", my_pid);
		s->data = malloc(s->segsize);
		printf("DEBUG %d: Done malloc'ing s->data\n", my_pid);
		//fread(s->data, s->segsize, 1, redo_fh);
		pread(redo_fh, s->data, s->segsize, offset);
		offset += s->segsize;
		printf("DEBUG %d: Read in data of %s\n", my_pid, (char *) s->data);
		char seg_file_path[512];
		strcpy(seg_file_path, rvm->prefix);
		strcat(seg_file_path, "/");
		strcat(seg_file_path, s->segname);
		int seg_file;
		seg_file = open(seg_file_path, O_WRONLY);
		for (i = 0; i < s->numupdates; i++) {
			//fwrite(&s->data+s->offsets[i], s->sizes[i], 1, seg_file);
			pwrite(seg_file, s->data+s->offsets[i], s->sizes[i], s->offsets[i]);
		}
		close(seg_file);
	}
	printf("DEBUG %d: Done with log truncation\n", my_pid);
}
