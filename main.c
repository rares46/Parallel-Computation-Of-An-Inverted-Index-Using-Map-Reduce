#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>

#define MAX_WORD 300
#define MAX_FILES 360
#define ALPHABET_SIZE 26
#define UNIQUE_WORDS 5000
#define INITIAL_CAPACITY 10

// structura pentru informatii despre fisiere
typedef struct FileInfo {
    char *file_name;
    long file_size;
    int id;
} FileInfo;

// functie de comparare a dimensiunilor fisierelor
int compare_file_sizes(const void *a, const void *b) {
    FileInfo *file1 = (FileInfo *)a;
    FileInfo *file2 = (FileInfo *)b;
    return file2->file_size - file1->file_size;
}

// structura pentru informatii despre cuvinte
typedef struct WordEntry {
    char *word;
    int *numbers;
    int num_count;
} WordEntry;

// structura pentru argumentele thread-urilor mappers
typedef struct {
    int id;
    int mapper_count;
    FileInfo file_list[MAX_FILES];
    int file_count;
    int *current_file_index;
    int start_file;
    int end_file;
    FILE *partial_outputs[ALPHABET_SIZE];
    int initial_files_id[MAX_FILES];
} MapperArgs;

// structura pentru argumentele thread-urilor reducers
typedef struct {
    int id;
    int reducer_count;
    FILE *input_files[ALPHABET_SIZE];
} ReducerArgs;

// functie de comparare a intrarilor de cuvinte
int compare_word_entries(const void *a, const void *b) {
    WordEntry *entry1 = (WordEntry *)a;
    WordEntry *entry2 = (WordEntry *)b;

    if (entry1->num_count != entry2->num_count) {
        return entry2->num_count - entry1->num_count;
    }

    return strcmp(entry1->word, entry2->word);
}

// adaugarea unui numar la un cuvant
void add_number(WordEntry *entry, int number) {
    if (entry->num_count % INITIAL_CAPACITY == 0) {
        int new_capacity = (entry->num_count + INITIAL_CAPACITY);
        entry->numbers = realloc(entry->numbers, new_capacity * sizeof(int));
        if (!entry->numbers) {
            fprintf(stderr, "Memory allocation failed for numbers array\n");
            exit(EXIT_FAILURE);
        }
    }
    entry->numbers[entry->num_count++] = number;
}

// eliberarea memoriei pentru un cuvant
void free_word_entry(WordEntry *entry) {
    free(entry->word);
    free(entry->numbers);
}

void *mapper(void *arg) {
    MapperArgs *args = (MapperArgs *)arg;
    if (args == NULL) {
        fprintf(stderr, "Error: NULL pointer passed to mapper\n");
    }
    int id = args->id;
    char buffer[MAX_WORD];

    for (int file_index = args->start_file; file_index < args->end_file; ++file_index) {
        char file_name[512];
        snprintf(file_name, sizeof(file_name), "%s", args->file_list[file_index].file_name);
        FILE *file = fopen(file_name, "r");
        if (!file) {
            fprintf(stderr, "Mapper %d: Error opening file %s\n", id, file_name);
            continue;
        }

        while (fscanf(file, "%s", buffer) == 1) {  // citire cuvant cu cuvant din fisier
            char clean_word[MAX_WORD] = {0};
            int j = 0;
            for (int i = 0; buffer[i] && j < MAX_WORD - 1; i++) {
                if (buffer[i] >= 'A' && buffer[i] <= 'Z') {  // daca e litera mare, o transforma in litera mica
                    clean_word[j++] = buffer[i] + 32;
                } else if (buffer[i] >= 'a' && buffer[i] <= 'z') {  // salveaza doar literele
                    clean_word[j++] = buffer[i];
                }
            }

            if (j > 0) {
                char first_letter = clean_word[0];
                if (first_letter >= 'a' && first_letter <= 'z') {  // adapteaza cuvantul la fisierul partial
                    fprintf(args->partial_outputs[first_letter - 'a'], "%s %d\n", clean_word, args->file_list[file_index].id + 1);
                }
            }
        }

        fclose(file);
    }
    pthread_exit(NULL);
}

void *reducer(void *arg) {
    ReducerArgs *args = (ReducerArgs *)arg;
    int id = args->id;
    int start_letter = (ALPHABET_SIZE / args->reducer_count) * id;  // calculeaza literele de la care incepe si se termina reducer-ul
    int end_letter = (id == args->reducer_count - 1) ? ALPHABET_SIZE : (ALPHABET_SIZE / args->reducer_count) * (id + 1);

    char filename[16];
    int j = start_letter;
    while (j < end_letter) {
        snprintf(filename, sizeof(filename), "partial_%c.txt", 'a' + j);
        args->input_files[j] = fopen(filename, "r");  // deschide fisierul partial
        j++;
    }

    char output_file_name[16];
    printf("REDUCER\n");

    for (int i = start_letter; i < end_letter; i++) {
        FILE *input_file = args->input_files[i];
        if (!input_file) {
            printf("eroare ca nu exista args->input_files[i]\n");
            continue;
        } 
        snprintf(output_file_name, sizeof(output_file_name), "%c.txt", 'a' + i);
        FILE *output_file = fopen(output_file_name, "w");
        if (!output_file) {
            fclose(input_file);
            printf("eroare la fisierul final\n");
            continue;
        }

        WordEntry *word_entries = NULL;  // dictionar de cuvinte
        int word_entry_count = 0;
        int word_entry_capacity = 10;
        word_entries = malloc(word_entry_capacity * sizeof(WordEntry));
        if (!word_entries) {
            fclose(input_file);
            fclose(output_file);
            printf("eroare la alocare entry\n");
            exit(EXIT_FAILURE);
        }

        char word[MAX_WORD];
        int number;
        while (fscanf(input_file, "%s %d", word, &number) == 2) {  // citire cuvant cu cuvant din fisierul partial
            int found = 0;
            for (int j = 0; j < word_entry_count; j++) {
                if (strcmp(word_entries[j].word, word) == 0) {  // se verifica daca cuvantul exista deja
                    found = 1;
                    int exists = 0;
                    for (int k = 0; k < word_entries[j].num_count; k++) {
                        if (word_entries[j].numbers[k] == number) {  // se verifica daca numarul pentru cuvantul respectiv exista deja
                            exists = 1;
                            break;
                        }
                    }
                    if (!exists) {
                        add_number(&word_entries[j], number);
                    }
                    break;
                }
            }

            if (!found) {
                if (word_entry_count == word_entry_capacity) {  // daca nu mai este loc in dictionar, se realoca
                    word_entry_capacity *= 2;
                    word_entries = realloc(word_entries, word_entry_capacity * sizeof(WordEntry));
                    if (!word_entries) {
                        fclose(input_file);
                        fclose(output_file);
                        printf("eroare la realocare\n");
                        exit(EXIT_FAILURE);
                    }
                }

                word_entries[word_entry_count].word = strdup(word);  // salveaza cuvantul
                word_entries[word_entry_count].numbers = malloc(INITIAL_CAPACITY * sizeof(int));
                if (!word_entries[word_entry_count].numbers) {  // se aloca memorie pentru numerele cuvantului
                    fclose(input_file);
                    fclose(output_file);
                    printf("eroare la alocare numere\n");
                    exit(EXIT_FAILURE);
                }
                word_entries[word_entry_count].num_count = 0;
                add_number(&word_entries[word_entry_count], number);  // adauga numarul la cuvant
                word_entry_count++;
            }
        }

        qsort(word_entries, word_entry_count, sizeof(WordEntry), compare_word_entries);  // sorteaza cuvintele

        for (int j = 0; j < word_entry_count; j++) {
            for (int a = 0; a < word_entries[j].num_count - 1; a++) {
                for (int b = a + 1; b < word_entries[j].num_count; b++) {
                    if (word_entries[j].numbers[a] > word_entries[j].numbers[b]) {  // sorteaza numerele
                        int aux = word_entries[j].numbers[a];
                        word_entries[j].numbers[a] = word_entries[j].numbers[b];
                        word_entries[j].numbers[b] = aux;
                    }
                }
            }
            fprintf(output_file, "%s:[", word_entries[j].word);  // scrie cuvantului in fisierul final
            for (int k = 0; k < word_entries[j].num_count; k++) {
                fprintf(output_file, "%d", word_entries[j].numbers[k]);  // scrierea numerelor in fisierul final
                if (k < word_entries[j].num_count - 1) {
                    fprintf(output_file, " ");
                }
            }
            fprintf(output_file, "]\n");
            free_word_entry(&word_entries[j]);
        }
        free(word_entries);
        fclose(input_file);
        fclose(output_file);
    }
    pthread_exit(NULL);
}



int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <num_mappers> <num_reducers> <input_file_list>\n", argv[0]);
        return -1;
    }

    int num_mappers = atoi(argv[1]);
    int num_reducers = atoi(argv[2]);
    char *input_file_list = argv[3];

    FILE *file_list = fopen(input_file_list, "r");  // fisierul cu lista de fisiere
    if (!file_list) {
        fprintf(stderr, "Error opening input file list: %s\n", input_file_list);
        return -1;
    }

    int file_count;
    if (fscanf(file_list, "%d", &file_count) != 1) {  // numarul de fisiere
        fprintf(stderr, "Error reading the number of files from input file list\n");
        fclose(file_list);
        return -1;
    }

    FileInfo files[MAX_FILES];
    long total_size = 0;

    for (int i = 0; i < file_count; ++i) {  // avem atatea structuri FileInfo cate fisiere
        files[i].file_name = malloc(MAX_WORD * sizeof(char));
        files[i].id = i;
        if (!files[i].file_name) {
            fprintf(stderr, "Memory allocation failed for file name\n");
            fclose(file_list);
            return -1;
        }
        if (fscanf(file_list, "%s", files[i].file_name) != 1) {
            fprintf(stderr, "Error reading file name from input file list\n");
            fclose(file_list);
            return -1;
        }

        char full_path[512];
        snprintf(full_path, sizeof(full_path), "%s", files[i].file_name);  // calea completa a fisierului
        struct stat st;
        if (stat(full_path, &st) == 0) {
            files[i].file_size = st.st_size;
            total_size += files[i].file_size;
        } else {
            fprintf(stderr, "Error getting size of file: %s\n", files[i].file_name);
            files[i].file_size = 0;
        }
    }
    fclose(file_list);

    qsort(files, file_count, sizeof(FileInfo), compare_file_sizes);  // sortarea fisierelor in functie de dimensiune

    int initial_files_id[file_count];
    for (int i = 0; i < file_count; i++) {
        initial_files_id[i] = files[i].id;
    }

    long size_per_mapper = total_size / num_mappers;
    int file_start[num_mappers];
    int file_end[num_mappers];
    long cumulative_size = 0;

    int current_mapper = 0;
    file_start[0] = 0;

    for (int i = 0; i < file_count; ++i) {
        cumulative_size += files[i].file_size;
        if (cumulative_size >= size_per_mapper && current_mapper < num_mappers - 1) {  // impartirea fisierelor pe mappere in functie de dimensiune
            file_end[current_mapper] = i;
            file_start[++current_mapper] = i + 1;
            cumulative_size = 0;
        }
    }
    file_end[current_mapper] = file_count - 1;


    for (int i = 0; i < num_mappers; ++i) {
        printf("Mapper %d: Files %d to %d\n", i, file_start[i], file_end[i] + 1);
    }

    int current_file_index = 0;

    FILE *partial_outputs[ALPHABET_SIZE];
    for (int i = 0; i < ALPHABET_SIZE; ++i) {
        char filename[16];
        snprintf(filename, sizeof(filename), "partial_%c.txt", 'a' + i);  // crearea fisierelor partiale
        partial_outputs[i] = fopen(filename, "w+");
        if (!partial_outputs[i]) {
            fprintf(stderr, "Error creating partial file: %s\n", filename);
            return -1;
        }
    }

    pthread_t mappers[num_mappers];
    MapperArgs mapper_args[num_mappers];
    ReducerArgs reducer_args[num_reducers];
    pthread_t reducers[num_reducers];

    for (int i = 0; i < num_mappers; i++) {
        int files_per_mapper = file_count / num_mappers;
        int remainder = file_count % num_mappers;
        mapper_args[i] = (MapperArgs){  // popularea argumentelor pentru mappers
            .id = i,
            .mapper_count = num_mappers,
            .start_file = file_start[i],
            .end_file = file_end[i] + 1,
            .current_file_index = &current_file_index,
        };
        for(int j = 0; j < ALPHABET_SIZE; j++) {
            mapper_args[i].partial_outputs[j] = partial_outputs[j];
        }
        for(int j = 0; j < file_count; j++) {
            mapper_args[i].file_list[j] = files[j];
        }
        pthread_create(&mappers[i], NULL, mapper, &mapper_args[i]);  // crearea thread-urilor mappers
    }

    for (int i = 0; i < num_mappers; ++i) {
        pthread_join(mappers[i], NULL);  // asteptarea thread-urilor mappers
    }

    for (int i = 0; i < ALPHABET_SIZE; ++i) {
        fclose(partial_outputs[i]);
    }

    for (int i = 0; i < num_reducers; ++i) {
        reducer_args[i] = (ReducerArgs){  // popularea argumentelor pentru reducers
            .id = i,
            .reducer_count = num_reducers,
        };
        pthread_create(&reducers[i], NULL, reducer, &reducer_args[i]);  // crearea thread-urilor reducers
    }
    for (int i = 0; i < num_reducers; ++i) {
        pthread_join(reducers[i], NULL);  // asteptarea thread-urilor reducers
    }

    for (int i = 0; i < file_count; ++i) {
        free(files[i].file_name);  // inchiderea fisierelor
    }
    return 0;
}