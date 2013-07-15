
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <dirent.h>
#include <mm_malloc.h>


#define MY_VERSION			"0.1"

#define MODE_READ			'r'
#define MODE_WRITE			'w'

// open flag for read
#define OPEN_FLAG_READ		(O_RDONLY|O_DIRECT)
// open flag for write
#define OPEN_FLAG_WRITE		(O_WRONLY|O_CREAT|O_TRUNC)


#define DEFAULT_PIECE_SIZE 	(256*1024)
//#define DEFAULT_MIN_SIZE 	(10*1024*1024)
//#define DEFAULT_MAX_SIZE 	(200*1024*1024)
#define DEFAULT_MIN_SIZE 	(1*1024*1024)
#define DEFAULT_MAX_SIZE 	(1*1024*1024)
#define DEFAULT_THREAD_NUM	10
#define DEFAULT_FILE_NUM_PER_THREAD 100
#define DEFAULT_DIR_NUM		1
#define DEFAULT_ROOT_PATH	"/media"
#define DEFAULT_START_INDEX 0
#define DEFAULT_MODE 		MODE_READ
#define DEFAULT_PIECE_NUM_PER_FILE 10
#define DEFAULT_TOTAL_NUM_PER_THREAD 50000

typedef struct config_t
{
	long piece_size;
	long min_size;
	long max_size;
	int thread_num;
	int	file_num_per_thread;
	int dir_num;
	char* root_path;	
	int	start_index;
	char mode;
	// 
	int piece_num_per_file;
	int	total_num_per_thread;
} CONFIG_T;

//stat: statistics.
typedef struct thread_stat_t
{
	int 	index;
	long	total_open_count;
	time_t	total_open_time;
	long	total_rw_count;
	time_t	total_rw_time; 
	long	total_close_count;
	time_t	total_close_time;
	int 	error;
} THREAD_STAT_T;

typedef struct file_t
{
	int 	index_in_thread;
	int 	index_in_process;
	int 	index_in_global;
	char 	full_name[PATH_MAX];
	long	size;
	int 	piece_num;
	int 	fd;
	char* 	bits; // bitfield
	int		bits_set_num;
	int 	error;
	// 		read N pieces
	int		start_index;
	int		read_num;
	int		read_eof;
} FILE_T;

CONFIG_T g_config = 
{
	/*.piece_size			=*/DEFAULT_PIECE_SIZE,
	/*.min_size 			= */DEFAULT_MIN_SIZE,
	/*.max_size 			= */DEFAULT_MAX_SIZE,
	/*.thread_num 		= */DEFAULT_THREAD_NUM,
	/*.file_num_per_thread=*/DEFAULT_FILE_NUM_PER_THREAD,
	/*.dir_num 			= */DEFAULT_DIR_NUM,
	/*.root_path 		= */DEFAULT_ROOT_PATH,
	/*.start_index 		= */DEFAULT_START_INDEX,
	/*.mode 			= */DEFAULT_MODE,
	/*.piece_num_per_file= */DEFAULT_PIECE_NUM_PER_FILE,
	/*.total_num_per_thread= */DEFAULT_TOTAL_NUM_PER_THREAD
};

// it must be aligned to the block size == 512.
//char g_data[PIECE_SIZE] = {0};
//static char g_data[PIECE_SIZE] __attribute__ ((__aligned__ (512)));
char* g_data = NULL;

int 	g_files_num = 0;
int 	g_files_size = 0;
char**  g_files = NULL;

void print_usage(char* program_name)
{
	fprintf(stdout, "%s --piece_size=%d --min=%d --max=%d --thread_num=%d --file_num_per_thread=%d --dir_num=%d "
		"--root_path=%s --start_index=%d --mode=%c --piece_num_per_file=%d --total_num_per_thread=%d\n",
		program_name, DEFAULT_PIECE_SIZE, DEFAULT_MIN_SIZE, DEFAULT_MAX_SIZE, DEFAULT_THREAD_NUM, DEFAULT_FILE_NUM_PER_THREAD, DEFAULT_DIR_NUM, 
		DEFAULT_ROOT_PATH, DEFAULT_START_INDEX, DEFAULT_MODE, DEFAULT_PIECE_NUM_PER_FILE, DEFAULT_TOTAL_NUM_PER_THREAD);	
	fprintf(stdout, "%s -z %d -i %d -a %d -t %d -f %d -d %d -r %s -s %d -m %c -p %d -n %d\n",
		program_name, DEFAULT_PIECE_SIZE, DEFAULT_MIN_SIZE, DEFAULT_MAX_SIZE, DEFAULT_THREAD_NUM, DEFAULT_FILE_NUM_PER_THREAD, DEFAULT_DIR_NUM, 
		DEFAULT_ROOT_PATH, DEFAULT_START_INDEX, DEFAULT_MODE, DEFAULT_PIECE_NUM_PER_FILE, DEFAULT_TOTAL_NUM_PER_THREAD);	

}

int data_buffer_init()
{
	g_data = (char*)_mm_malloc(g_config.piece_size, 512);
	if(g_data == NULL)
	{
		return -1;
	}

	return 0;
}

void memory_release()
{
	if(g_data != NULL)
	{
		_mm_free(g_data);
		g_data = NULL;
	}
	if(g_files != NULL)
	{
		g_files_num = 0;
		g_files_size= 0;
		free(g_files);
		g_files = NULL;
	}
}

int try_make_dir(char* dir)
{
	int ret = 0;

	if(access(dir, F_OK) == 0)
	{
	     return 0;
	}
		    
	mode_t pre_mode = umask(0);
	ret = mkdir(dir, 0755);
	if(ret < 0)
	{
		fprintf(stderr, "%s: mkdir errno %d, %s\n", __FUNCTION__, errno, strerror(errno));
	}
	umask(pre_mode);
	
	return ret;
}

long rand_index(long radix)
{
	if(radix == 0)
	{
		return 0;
	}
	
	long rand_val = random();
	long ret_val = rand_val%radix;
	return ret_val;
}

long rand_size()
{
	if(g_config.max_size == g_config.min_size)
	{
		return g_config.max_size;
	}
	
	long rand_val = random();
	long ret_val = rand_val%(g_config.max_size-g_config.min_size) + g_config.min_size;
	long ret_val2 = (ret_val + g_config.piece_size - 1)/g_config.piece_size*g_config.piece_size;
	return ret_val2;
}

time_t timeval_diff(struct timeval* t2, struct timeval* t1)
{
	time_t ret1 = 0;
	time_t ret2 = 0;
	ret1 = t2->tv_sec - t1->tv_sec;
	ret2 = ret1 * 1000000 + t2->tv_usec - t1->tv_usec;
	return ret2;
}

int bitfield_find_unset(char* bits, int piece_num, int start_index)
{
	int pos = 0;
	int index = 0;
	for(index=0; index<piece_num; index++)
	{
		pos = index+start_index;
		if(pos>=piece_num)
		{
			pos = pos%piece_num;
		}

		int byte_pos = pos/8;
		int bit_pos = pos%8;
		int bit_pos2 = 7 - bit_pos;
		if(!(bits[byte_pos] & (1<<bit_pos2)))
		{
			// find it
			return pos;
		}
	}

	return -1;
}

int bitfield_set_one(char* bits, int piece_num, int index)
{
	int byte_pos = index/8;
	int bit_pos = index%8;
	int bit_pos2 = 7 - bit_pos;
	bits[byte_pos] = bits[byte_pos] | (1<<bit_pos2);

	return 0;
}


int files_find_unfinish(FILE_T* files, int start_index)
{
	int pos = 0;
	int index = 0;
	for(index=0; index<g_config.file_num_per_thread; index++)
	{
		pos = index+start_index;
		if(pos>=g_config.file_num_per_thread)
		{
			pos = pos%g_config.file_num_per_thread;
		}
		if((!files[pos].error) && files[pos].bits_set_num < files[pos].piece_num)
		{
			// find it
			return pos;
		}
	}

	return -1;
	
}

int file_is_finish(FILE_T* filep)
{
	if(filep->error || (filep->bits_set_num == filep->piece_num) )
	{
		return 1; //true
	}

	return 0; // false
}

int file_read_piece(FILE_T* filep, time_t* timep)
{	
	int ret = 0;
	
	// read piece by random
	int piece_index = rand_index(filep->piece_num);
	int piece_pos = bitfield_find_unset(filep->bits, filep->piece_num, piece_index);
	if(piece_pos == -1)
	{
		return -1;
	}

	struct timeval t1, t2;		
	gettimeofday(&t1, NULL);
	ret = lseek(filep->fd, g_config.piece_size*piece_pos, SEEK_SET);		
	if(ret == -1)
	{
		filep->error = 1;
		return -1;
	}

	ret = read(filep->fd, g_data, g_config.piece_size);	
	if(ret < g_config.piece_size)
	{
		fprintf(stderr, "%s: read %s, want=%ld, ret=%d, errno=%d, %s\n",
			__FUNCTION__, filep->full_name, g_config.piece_size, ret, errno, strerror(errno));
		filep->error = 1;
		return -1;
	}
	gettimeofday(&t2, NULL);
	*timep = timeval_diff(&t2, &t1);

	filep->bits_set_num ++;
	bitfield_set_one(filep->bits, filep->piece_num, piece_pos);
	
	return piece_pos;
}


int file_write_piece(FILE_T* filep, time_t* timep)
{	
	int ret = 0;
	
	// write piece by random
	int piece_index = rand_index(filep->piece_num);
	int piece_pos = bitfield_find_unset(filep->bits, filep->piece_num, piece_index);
	if(piece_pos == -1)
	{
		return -1;
	}

	/*
	char data[g_config.piece_size];	
	int data_index = 0;
	for(data_index=0; data_index<g_config.piece_size; data_index++)
	{
		data[data_index] = rand_index(256);
	}
	*/

	struct timeval t1, t2;		
	gettimeofday(&t1, NULL);
	ret = lseek(filep->fd, g_config.piece_size*piece_pos, SEEK_SET);	
	if(ret == -1)
	{
		filep->error = 1;
		return -1;
	}
	//write(filep->fd, data, g_config.piece_size);
	ret = write(filep->fd, g_data, g_config.piece_size);
	if(ret < g_config.piece_size)
	{
		fprintf(stderr, "%s: write %s, want=%ld, ret=%d, errno=%d, %s\n",
			__FUNCTION__, filep->full_name, g_config.piece_size, ret, errno, strerror(errno));
		filep->error = 1;
		return -1;
	}
	gettimeofday(&t2, NULL);
	*timep = timeval_diff(&t2, &t1);

	filep->bits_set_num ++;
	bitfield_set_one(filep->bits, filep->piece_num, piece_pos);
	
	return piece_pos;
}

int data_random()
{
	int data_index = 0;
	for(data_index=0; data_index<g_config.piece_size; data_index++)
	{
		g_data[data_index] = rand_index(256);
	}

	return 0;
}

int path_is_file(char* path)
{
	struct stat st;
	lstat(path, &st);
	return S_ISREG(st.st_mode);
}

int files_add_one(char* file_name)
{
	if(g_files_size <= g_files_num)
	{
		//fprintf(stdout, "%s: g_files_size=%d, g_files_num=%d before\n", __FUNCTION__, g_files_size, g_files_num);
		char** temp = (char**)realloc(g_files, sizeof(char*) * (g_files_size+g_config.file_num_per_thread) );
		if(temp == NULL)
		{
			fprintf(stderr, "%s: realloc failed! previous size=%d, enlarge size=%d\n", __FUNCTION__, g_files_size, g_files_size+g_config.file_num_per_thread);
			return -1;
		}

		g_files = temp;
		g_files_size = g_files_size + g_config.file_num_per_thread;		
		//fprintf(stdout, "%s: g_files_size=%d, g_files_num=%d after\n", __FUNCTION__, g_files_size, g_files_num);
	}
	
	g_files[g_files_num] = strdup(file_name);
	g_files_num ++;

	//fprintf(stdout, "%s: g_files_num=%d, %s\n", __FUNCTION__, g_files_num, file_name);

	return 0;	
}

int dir_scan(char* path)
{	
    DIR* dirp = opendir(path);
    if(dirp == NULL)
    {        
        return -1;
    }

    char temp[PATH_MAX] = {0}; 
    struct dirent *direntp = NULL;
    while((direntp=readdir(dirp))!=NULL)
    {
        if(strcmp(direntp->d_name, ".") == 0
           || strcmp(direntp->d_name, "..") == 0)
        {
            continue;
        }
        sprintf(temp, "%s/%s", path, direntp->d_name);
        if ( path_is_file(temp))
        {
        	files_add_one(temp);
        }
        
    }
    closedir(dirp);

    fprintf(stdout, "%s: g_files_num=%d\n", __FUNCTION__, g_files_num);
    
	return 0;
}

int disk_scan()
{
	int index = 0;
	for(index=0; index<g_config.dir_num; index++)
	{
		char path[PATH_MAX];
		snprintf(path, PATH_MAX-1, "%s/dir%d", g_config.root_path, index);
		path[PATH_MAX-1] = '\0';
		dir_scan(path);
	}

	return 0;
}

void* test_read(void* arg)
{
	THREAD_STAT_T* statp = (THREAD_STAT_T*)arg;

	FILE_T* files = (FILE_T*)malloc(g_config.file_num_per_thread*sizeof(FILE_T));
	if(files == NULL)
	{
		fprintf(stderr, "%s: malloc for FILE_T failed\n", __FUNCTION__);
		statp->error = 1;
		return NULL;
	}
	memset(files, 0, g_config.file_num_per_thread*sizeof(FILE_T));
	
	int index = 0;
	for(index=0; index<g_config.file_num_per_thread; index++)
	{
		FILE_T& file = files[index];
		file.index_in_thread  = index;
		file.index_in_process = statp->index * g_config.file_num_per_thread + file.index_in_thread;
		file.index_in_global  = g_config.start_index + file.index_in_process;
		int dir_index = file.index_in_process/(g_config.thread_num*g_config.file_num_per_thread/g_config.dir_num);
		snprintf(file.full_name, PATH_MAX-1, "%s/dir%d/file%d", g_config.root_path, dir_index, file.index_in_global);
		file.full_name[PATH_MAX-1] = '\0';
		
		struct timeval t1, t2;		
		gettimeofday(&t1, NULL);
		file.fd = open(file.full_name, OPEN_FLAG_READ);
		if(file.fd == -1)
		{
			fprintf(stderr, "%s: open %s failed\n", __FUNCTION__, file.full_name);
			statp->error = 1;
			return NULL;
		}
		gettimeofday(&t2, NULL);
		time_t open_time = timeval_diff(&t2, &t1);
		fprintf(stdout, "%s: open %s[%ld] spend %ld us\n", __FUNCTION__, file.full_name, file.size, open_time);
		
		file.size = lseek(file.fd, 0, SEEK_END);;
		file.piece_num = file.size/g_config.piece_size;
		int byte_num = (g_config.piece_size+8-1)/8*8;
		file.bits = (char*)malloc(byte_num);
		if(file.bits == NULL)
		{
			fprintf(stderr, "%s: malloc for byte_num failed\n", __FUNCTION__);
			statp->error = 1;
			return NULL;
		}
		memset(file.bits, 0, byte_num);
		file.bits_set_num = 0;
		
		statp->total_open_time += open_time;

		lseek(file.fd, 0, SEEK_SET);		
	}

	// choose one file by random
	int finish_file_num = 0;
	while(finish_file_num < g_config.file_num_per_thread)
	{
		//for(index=0; index<g_config.file_num_per_thread; index++)
		//{
			int file_index = 0;
			file_index = rand_index(g_config.file_num_per_thread);
			int pos = files_find_unfinish(files, file_index);
			if(pos < 0)
			{
				break;
			}
			FILE_T& file = files[pos];
			time_t read_time = 0;
			int piece_index = file_read_piece(&file, &read_time);
			fprintf(stdout, "%s: read %s[%d] spend %ld us\n", __FUNCTION__, file.full_name, piece_index, read_time);

			if(piece_index != -1)
			{
				statp->total_rw_count ++;
				statp->total_rw_time += read_time;
			}
			if(file.error)
			{
				statp->error = 1;
			}
			
			if(file_is_finish(&file))
			{
				//close(filep->fd);
				//filep->fd == -1;
				fprintf(stdout, "%s: %s finished\n", __FUNCTION__, file.full_name);
				finish_file_num ++;				
			}
		//}
	}

	for(index=0; index<g_config.file_num_per_thread; index++)
	{
		FILE_T& file = files[index];
		
		struct timeval t1, t2;		
		gettimeofday(&t1, NULL);
		close(file.fd);
		file.fd = -1;
		gettimeofday(&t2, NULL);
		time_t close_time = timeval_diff(&t2, &t1);
		fprintf(stdout, "%s: close %s[%ld] spend %ld us\n", __FUNCTION__, file.full_name, file.size, close_time);
		
		statp->total_close_time += close_time;
	}
	
	return NULL;
}

void* test_read2(void* arg)
{
	THREAD_STAT_T* statp = (THREAD_STAT_T*)arg;

	FILE_T* files = (FILE_T*)malloc(g_config.file_num_per_thread*sizeof(FILE_T));
	if(files == NULL)
	{
		fprintf(stderr, "%s: malloc for FILE_T failed\n", __FUNCTION__);
		statp->error = 1;
		return NULL;
	}
	memset(files, 0, g_config.file_num_per_thread*sizeof(FILE_T));

	// choose files, by random.
	int index = 0;
	for(index=0; index<g_config.file_num_per_thread; index++)
	{
		FILE_T& file = files[index];
		int global_file_index = rand_index(g_files_num);
		snprintf(file.full_name, PATH_MAX-1, "%s", g_files[global_file_index]);
		file.full_name[PATH_MAX-1] = '\0';
		
		struct timeval t1, t2;		
		gettimeofday(&t1, NULL);
		file.fd = open(file.full_name, OPEN_FLAG_READ);
		if(file.fd == -1)
		{
			fprintf(stderr, "%s: open %s failed\n", __FUNCTION__, file.full_name);
			statp->error = 1;
			return NULL;
		}
		gettimeofday(&t2, NULL);
		time_t open_time = timeval_diff(&t2, &t1);				
		statp->total_open_count ++;
		statp->total_open_time += open_time;			
		
		file.size = lseek(file.fd, 0, SEEK_END);
		file.piece_num = file.size/g_config.piece_size;
		file.start_index = rand_index(file.piece_num);
		file.read_num = 0;
		file.read_eof = 0;	
		lseek(file.fd, file.start_index*g_config.piece_size, SEEK_SET);
		fprintf(stdout, "%s: open %s[%d][%d] spend %ld us\n", __FUNCTION__, file.full_name, file.piece_num, file.start_index, open_time);
		
	}

	// choose one file by random	
	while(statp->total_rw_count <= g_config.total_num_per_thread)
	{		
		int file_index = 0;
		file_index = rand_index(g_config.file_num_per_thread);		
		FILE_T& file = files[file_index];
		// file read one piece.
		// if read_eof or read_num >= g_config.piece_num_per_file
		//		close this file, find another file, open it, seek it, 
		time_t read_time = 0;
		struct timeval t1, t2;		
		gettimeofday(&t1, NULL);		
		int ret = read(file.fd, g_data, g_config.piece_size);	
		if(ret < g_config.piece_size)
		{
			file.read_eof = 1;
			fprintf(stdout, "%s: read %s[%d][%d+%d] get error ret=%d, errno=%d, %s\n", 
				__FUNCTION__, file.full_name, file.piece_num, file.start_index, file.read_num, ret, errno, strerror(errno));
		}
		else
		{
			file.read_num ++;
			gettimeofday(&t2, NULL);
			read_time = timeval_diff(&t2, &t1);
			fprintf(stdout, "%s: read %s[%d][%d+%d] spend %ld us\n", __FUNCTION__, file.full_name, file.piece_num, file.start_index, file.read_num, read_time);
			statp->total_rw_count ++;
			statp->total_rw_time += read_time;
		}

		if(file.read_eof || (file.read_num >= g_config.piece_num_per_file))
		{
			// close this file.
			//struct timeval t1, t2;		
			gettimeofday(&t1, NULL);
			if(file.fd != -1)
			{
				close(file.fd);
				file.fd = -1;
			}
			gettimeofday(&t2, NULL);
			time_t close_time = timeval_diff(&t2, &t1);
			fprintf(stdout, "%s: close %s[%d][%d+%d] spend %ld us\n", __FUNCTION__, file.full_name, file.piece_num, file.start_index, file.read_num, close_time);
			statp->total_close_count ++;
			statp->total_close_time += close_time;

			// find another file.
			int global_file_index = rand_index(g_files_num);
			snprintf(file.full_name, PATH_MAX-1, "%s", g_files[global_file_index]);
			file.full_name[PATH_MAX-1] = '\0';
			// open it.
			//struct timeval t1, t2;		
			gettimeofday(&t1, NULL);
			file.fd = open(file.full_name, OPEN_FLAG_READ);
			if(file.fd == -1)
			{
				fprintf(stderr, "%s: open %s failed\n", __FUNCTION__, file.full_name);
				statp->error = 1;
				return NULL;
			}
			gettimeofday(&t2, NULL);
			time_t open_time = timeval_diff(&t2, &t1);		
			statp->total_open_count ++;
			statp->total_open_time += open_time;			
			
			file.size = lseek(file.fd, 0, SEEK_END);
			file.piece_num = file.size/g_config.piece_size;
			file.start_index = rand_index(file.piece_num);
			file.read_num = 0;
			file.read_eof = 0;	
			lseek(file.fd, file.start_index*g_config.piece_size, SEEK_SET);
			fprintf(stdout, "%s: open %s[%d][%d] spend %ld us\n", __FUNCTION__, file.full_name, file.piece_num, file.start_index, open_time);
		}
		
		
	}

	for(index=0; index<g_config.file_num_per_thread; index++)
	{
		FILE_T& file = files[index];
		
		struct timeval t1, t2;		
		gettimeofday(&t1, NULL);
		if(file.fd != -1)
		{
			close(file.fd);
			file.fd = -1;
		}
		gettimeofday(&t2, NULL);
		time_t close_time = timeval_diff(&t2, &t1);
		fprintf(stdout, "%s: close %s[%d][%d+%d] spend %ld us\n", __FUNCTION__, file.full_name, file.piece_num, file.start_index, file.read_num, close_time);
		
		statp->total_close_count ++;
		statp->total_close_time += close_time;
	}
	
	return NULL;
}


void* test_write(void* arg)
{
	THREAD_STAT_T* statp = (THREAD_STAT_T*)arg;

	FILE_T* files = (FILE_T*)malloc(g_config.file_num_per_thread*sizeof(FILE_T));
	if(files == NULL)
	{
		fprintf(stderr, "%s: malloc for FILE_T failed\n", __FUNCTION__);
		statp->error = 1;
		return NULL;
	}
	memset(files, 0, g_config.file_num_per_thread*sizeof(FILE_T));
	
	int index = 0;
	for(index=0; index<g_config.file_num_per_thread; index++)
	{
		FILE_T& file = files[index];
		file.index_in_thread  = index;
		file.index_in_process = statp->index * g_config.file_num_per_thread + file.index_in_thread;
		file.index_in_global  = g_config.start_index + file.index_in_process;
		int dir_index = file.index_in_process/(g_config.thread_num*g_config.file_num_per_thread/g_config.dir_num);
		if(dir_index<g_config.dir_num)
		{
			// do nothing.
		}
		else
		{
			dir_index = g_config.dir_num - 1;
		}
		snprintf(file.full_name, PATH_MAX-1, "%s/dir%d/file%d", g_config.root_path, dir_index, file.index_in_global);
		file.full_name[PATH_MAX-1] = '\0';
		file.size = rand_size();
		file.piece_num = file.size/g_config.piece_size;
		int byte_num = (g_config.piece_size+8-1)/8*8;
		file.bits = (char*)malloc(byte_num);
		if(file.bits == NULL)
		{
			fprintf(stderr, "%s: malloc for byte_num failed\n", __FUNCTION__);
			statp->error = 1;
			return NULL;
		}
		memset(file.bits, 0, byte_num);
		file.bits_set_num = 0;

		struct timeval t1, t2;		
		gettimeofday(&t1, NULL);
		file.fd = open(file.full_name, OPEN_FLAG_WRITE, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
		if(file.fd == -1)
		{
			fprintf(stderr, "%s: open %s failed\n", __FUNCTION__, file.full_name);
			statp->error = 1;
			return NULL;
		}
		gettimeofday(&t2, NULL);
		time_t open_time = timeval_diff(&t2, &t1);
		fprintf(stdout, "%s: open %s[%ld] spend %ld us\n", __FUNCTION__, file.full_name, file.size, open_time);
		
		statp->total_open_time += open_time;

		off_t ret = lseek(file.fd, file.size, SEEK_CUR);
		if(ret == -1)
		{
			fprintf(stderr, "%s: lseek %s[%ld] errno=%d, %s\n", __FUNCTION__, file.full_name, file.size, errno, strerror(errno));
			statp->error = 1;
			return NULL;
		}
	}

	// choose one file by random
	int finish_file_num = 0;
	while(finish_file_num < g_config.file_num_per_thread)
	{
		//for(index=0; index<g_config.file_num_per_thread; index++)
		//{
			int file_index = 0;
			file_index = rand_index(g_config.file_num_per_thread);
			int pos = files_find_unfinish(files, file_index);
			if(pos < 0)
			{
				break;
			}
			FILE_T& file = files[pos];
			time_t write_time = 0;
			int piece_index = file_write_piece(&file, &write_time);
			fprintf(stdout, "%s: write %s[%d] spend %ld us\n", __FUNCTION__, file.full_name, piece_index, write_time);

			if(piece_index != -1)
			{
				statp->total_rw_count ++;
				statp->total_rw_time += write_time;
			}
			if(file.error)
			{
				statp->error = 1;
			}
			
			if(file_is_finish(&file))
			{
				//close(filep->fd);
				//filep->fd == -1;
				fprintf(stdout, "%s: %s finished\n", __FUNCTION__, file.full_name);
				finish_file_num ++;				
			}
		//}
	}

	for(index=0; index<g_config.file_num_per_thread; index++)
	{
		FILE_T& file = files[index];
		
		struct timeval t1, t2;		
		gettimeofday(&t1, NULL);
		close(file.fd);
		file.fd = -1;
		gettimeofday(&t2, NULL);
		time_t close_time = timeval_diff(&t2, &t1);
		fprintf(stdout, "%s: close %s[%ld] spend %ld us\n", __FUNCTION__, file.full_name, file.size, close_time);
		
		statp->total_close_time += close_time;
	}
	
	return NULL;
}

void* direct_write(void* arg)
{
	THREAD_STAT_T* statp = (THREAD_STAT_T*)arg;

	FILE_T* files = (FILE_T*)malloc(g_config.file_num_per_thread*sizeof(FILE_T));
	if(files == NULL)
	{
		fprintf(stderr, "%s: malloc for FILE_T failed\n", __FUNCTION__);
		statp->error = 1;
		return NULL;
	}
	memset(files, 0, g_config.file_num_per_thread*sizeof(FILE_T));
	
	int index = 0;
	for(index=0; index<g_config.file_num_per_thread; index++)
	{
		FILE_T& file = files[index];
		file.index_in_thread  = index;
		file.index_in_process = statp->index * g_config.file_num_per_thread + file.index_in_thread;
		file.index_in_global  = g_config.start_index + file.index_in_process;
		int dir_index = file.index_in_process/(g_config.thread_num*g_config.file_num_per_thread/g_config.dir_num);
		if(dir_index<g_config.dir_num)
		{
			// do nothing.
		}
		else
		{
			dir_index = g_config.dir_num - 1;
		}
		snprintf(file.full_name, PATH_MAX-1, "%s/dir%d/file%d", g_config.root_path, dir_index, file.index_in_global);
		file.full_name[PATH_MAX-1] = '\0';
		file.size = rand_size();
		file.piece_num = file.size/g_config.piece_size;
		int byte_num = (g_config.piece_size+8-1)/8*8;
		file.bits = (char*)malloc(byte_num);
		if(file.bits == NULL)
		{
			fprintf(stderr, "%s: malloc for byte_num failed\n", __FUNCTION__);
			statp->error = 1;
			return NULL;
		}
		memset(file.bits, 0, byte_num);
		file.bits_set_num = 0;
		
		file.fd = open(file.full_name, OPEN_FLAG_WRITE, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
		if(file.fd == -1)
		{
			fprintf(stderr, "%s: open %s failed\n", __FUNCTION__, file.full_name);
			statp->error = 1;
			return NULL;
		}		
		fprintf(stdout, "%s: open %s[%ld]\n", __FUNCTION__, file.full_name, file.size);
				
		int piece_index = 0;
		for(piece_index=0; piece_index<file.piece_num; piece_index++)
		{
			int ret = write(file.fd, g_data, g_config.piece_size);
			if(ret < g_config.piece_size)
			{
				file.error = 1;
				fprintf(stderr, "%s: write %s failed, want=%ld, ret=%d, errno=%d, %s\n", 
					__FUNCTION__, file.full_name, g_config.piece_size, ret, errno, strerror(errno));
				break;
			}
		}

		if(file.fd != -1)
		{
			close(file.fd);
			file.fd = -1;
			fprintf(stdout, "%s: close %s[%ld]\n", __FUNCTION__, file.full_name, file.size);
		}
		
	}
	
	return NULL;
}


int stat_threads(THREAD_STAT_T* stats)
{
	long   total_open_count = 0;
	time_t total_open_time = 0;
	long   total_rw_count = 0;
	time_t total_rw_time = 0;
	long   total_close_count = 0;
	time_t total_close_time = 0;

	time_t average_open_time = 0;
	time_t average_rw_time = 0;
	time_t average_close_time = 0;
	
	int index = 0;
	for(index=0; index<g_config.thread_num; index++)
	{
		THREAD_STAT_T& stat = stats[index];
		if(stat.error)
		{
			fprintf(stderr, "%s: error occured!\n", __FUNCTION__);
			return -1;
		}

		total_open_count += stat.total_open_count;
		total_open_time += stat.total_open_time;
		total_rw_count += stat.total_rw_count;
		total_rw_time += stat.total_rw_time;
		total_close_count += stat.total_close_count;
		total_close_time += stat.total_close_time;		
	}

	if(total_open_count > 0)
	{
		average_open_time = total_open_time/total_open_count;
	}
	if(total_rw_count > 0)
	{
		average_rw_time   = total_rw_time/total_rw_count;
	}
	if(total_close_count > 0)
	{
		average_close_time = total_close_time/total_close_count;
	}
	fprintf(stdout, "%s: average_open_time=%ld average_rw_time=%ld average_close_time=%ld\n", __FUNCTION__, average_open_time, average_rw_time, average_close_time);
	
	return 0;
}

int main(int argc, char* argv[])
{
	int ret = 0;
		
	// parse the command line.
    bool  have_unknown_opts = false;
    // command line
    // -z, --piece_size
    // -i,  --min
    // -a, --max
    // -t, --thread_num
    // -f, --file_num_per_thread
    // -d, --dir_num
    // -r, --root_path
    // -s, --start_index
    // -m, --mode
	// -p, --piece_num_per_file    
	// -n, --total_num_per_thread    
    // -v, --version
    // -h, --help
    // parse_cmd_line();
    static struct option orig_options[] = 
    {
    	{ "piece_size",	  1, 0, 'z' },
        { "min",       	  1, 0, 'i' },
        { "max",       	  1, 0, 'a' },
        { "thread_num",   1, 0, 't' },
        { "file_num_per_thread", 1, 0, 'f' }, 
        { "dir_num",      1, 0, 'd' },
        { "root_path",    1, 0, 'r' },
        { "start_index",  1, 0, 's' },
        { "mode",  		  1, 0, 'm' },
        { "piece_num_per_file", 1, 0, 'p' },
        { "total_num_per_thread", 1, 0, 'n' },
        { "version",      0, 0, 'v' },
        { "help",         0, 0, 'h' },        
        { NULL,           0, 0, 0   }
	};	
	while (true) 
	{
	    int c = -1;
	    int option_index = 0;
	  
	    c = getopt_long_only(argc, argv, "z:i:a:t:f:d:r:s:m:p:n:vh", orig_options, &option_index);
    	if (c == -1)
	        break;

	    switch (c) 
	    {
	    	case 'z':	
	            g_config.piece_size = atol(optarg);
	            break;
	        case 'i':	
	            g_config.min_size = atol(optarg);
	            break;
	        case 'a':	
	            g_config.max_size = atol(optarg);
	            break;
	        case 't':	
	            g_config.thread_num = atoi(optarg);
	            break;
	        case 'f':	
	            g_config.file_num_per_thread = atoi(optarg);
	            break;
	        case 'd':	
	            g_config.dir_num = atoi(optarg);
	            break;
	        case 'r':	
	            g_config.root_path = strdup(optarg);
	            break;	       
	        case 's':	
	            g_config.start_index = atoi(optarg);
	            break;
	        case 'm':	
	            g_config.mode = optarg[0];
	            break;
	        case 'p':	
	            g_config.piece_num_per_file = atoi(optarg);
	            break;
            case 'n':	
	            g_config.total_num_per_thread = atoi(optarg);
	            break;
	        case 'h':
	            print_usage(argv[0]);	            	    
	            exit(0);
	            break;	          
	        case 'v':
	            fprintf(stdout, "%s: version %s\n", argv[0], MY_VERSION);
        	    exit(0);
        	    break;
	        case '?':
	        default:
	            have_unknown_opts = true;
	            break;
	    }
    }

	// srandom();
	srandom(time(NULL));
	ret = data_buffer_init();
	if(ret != 0)
	{
		fprintf(stdout, "%s: data_buffer_init failed! so exit\n", __FUNCTION__);
		exit(-1);
	}
	
	// create dir, dir_num
	int index = 0;
	for(index=0; index<g_config.dir_num; index++)
	{
		char path[PATH_MAX];
		snprintf(path, PATH_MAX-1, "%s/dir%d", g_config.root_path, index);
		path[PATH_MAX-1] = '\0';
		try_make_dir(path);
	}
	
	// create thread, thread_num
	typedef void* (*start_routine)(void*);
	start_routine funcp = NULL;
	if(g_config.mode == MODE_READ)
	{
		disk_scan();		
		funcp = test_read2;
	}
	else if(g_config.mode == MODE_WRITE)
	{
		data_random();
		//funcp = test_write;
		funcp = direct_write;
	}
	else
	{
		// error
		fprintf(stderr, "%s: mode only support %c or %c\n", __FUNCTION__, MODE_READ, MODE_WRITE);
		return -1;
	}

	pthread_t* threads = (pthread_t*)malloc(g_config.thread_num*sizeof(pthread_t));
	if(threads == NULL)
	{
		// error
		fprintf(stderr, "%s: malloc for pthread_t failed\n", __FUNCTION__);
		return -1;
	}
	memset(threads, 0, g_config.thread_num*sizeof(pthread_t));

	THREAD_STAT_T* stats = (THREAD_STAT_T*)malloc(g_config.thread_num*sizeof(THREAD_STAT_T));
	if(stats == NULL)
	{
		// error
		fprintf(stderr, "%s: malloc for THREAD_STAT_T failed\n", __FUNCTION__);
		return -1;
	}
	memset(stats, 0, g_config.thread_num*sizeof(THREAD_STAT_T));
	
	for(index=0; index<g_config.thread_num; index++)
	{
		stats[index].index = index;
		ret = pthread_create(&(threads[index]), NULL, funcp, (void*)(&stats[index]));
		if(ret < 0)
		{
			// error
			fprintf(stderr, "%s: pthread_create %d failed\n", __FUNCTION__, index);
			return -1;
		}
	}

	for(index=0; index<g_config.thread_num; index++)
	{
		pthread_join(threads[index], NULL);
	}

	ret = stat_threads(stats);
	if(ret == 0)
	{
		fprintf(stdout, "%s: test success!\n", __FUNCTION__);
	}
	else
	{
		fprintf(stdout, "%s: test failure!\n", __FUNCTION__);
	}
	
	return ret;
}

