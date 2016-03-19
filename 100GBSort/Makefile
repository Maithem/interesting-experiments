CC = gcc -Ofast -std=gnu99

GENERATE_DATA = "generate"
READ_CHUNK = "read"
VERIFY_DATA = "verify"
SHUFFLE_DATA = "shuffle"
EXTERNAL_SORT = "gigasort"

build: 
	$(CC) -g -o $(GENERATE_DATA) generate_data.c
	$(CC) -g -o $(READ_CHUNK) read_chunk.c
	$(CC) -g -o $(VERIFY_DATA) verify_data.c shared.c
	$(CC) -g -o $(SHUFFLE_DATA) shuffle_data.c shared.c
	$(CC) -g -o $(EXTERNAL_SORT) external_sort.c shared.c

clean:
	-rm -rf  $(EXTERNAL_SORT) $(GENERATE_DATA) $(READ_CHUNK) $(VERIFY_DATA) $(SHUFFLE_DATA)
