#ifndef SHARED_H
#define SHARED_H


typedef struct FilesDirectory {
    char **file_paths;
    unsigned int len;
} FilesDirectory;

FilesDirectory *read_dir(const char *dir_path);

unsigned nCk( unsigned n, unsigned k );

void shuffle(int64_t *a, int64_t n);

#endif