#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <semaphore.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdint.h>


#define QUEUE_CAPACITY 10

void exit_with_sys(char *s)
{
    perror(s);
    exit(EXIT_FAILURE);
}

void exit_with_sys_msg(char *s, int e)
{
    fprintf(stderr, "%s :%s", s, strerror(e));
    exit(EXIT_FAILURE);
}

typedef struct
{
    int16_t vec[8];
    unsigned int file_id;
    unsigned long vector_num;
    bool is_poison_pill;


}queue_item_t;

typedef struct
{
    uint32_t dichiarati;
    unsigned long equisomma;
    unsigned long non_equisomma;
    unsigned long totali;

}file_stats_t;

typedef struct
{
    queue_item_t items[QUEUE_CAPACITY];
    unsigned int head;
    unsigned int tail;

    sem_t empty_slots;
    sem_t full_slots;
    pthread_mutex_t mutex;

} safe_queue_t;

typedef struct 
{
    safe_queue_t queue;
    char **filenames;

    pthread_mutex_t stast_lock;
    file_stats_t *stats;

    unsigned int num_files;
    unsigned int num_verifiers;
}shared_context_t;

typedef struct 
{
    unsigned int file_idx;
    shared_context_t *shared;
} reader_arg_t;

typedef struct
{
    unsigned int verifier_id;
    shared_context_t *shared;
} verifier_arg_t;

void queue_init(safe_queue_t *q)
{
    assert(q!= NULL);
    
    int err;

    q->head = 0;
    q->tail = 0;

    if((err = pthread_mutex_init(&q->mutex, NULL)))
        exit_with_sys_msg("mutex init",err);

    if(sem_init(&q->empty_slots,0,QUEUE_CAPACITY))
        exit_with_sys("empty_slots init");

    if(sem_init(&q->full_slots,0,0))
        exit_with_sys("full_slots init");

}

void queue_destroy(safe_queue_t *q)
{
    assert(q!= NULL);
    int err;

    if((err = pthread_mutex_destroy(&q->mutex)))
        exit_with_sys_msg("mutex init",err);

    if(sem_destroy(&q->empty_slots))
        exit_with_sys("empty_slots destroy");

    if(sem_destroy(&q->full_slots))
        exit_with_sys("full_slots destroy");
}

void queue_push(safe_queue_t *q, queue_item_t item)
{
    assert(q != NULL);
    int err;

    if(sem_wait(&q->empty_slots))
        exit_with_sys("empty_slots wait");

    if((err = pthread_mutex_lock(&q->mutex)))
        exit_with_sys_msg("mutex lock",err);

    q->items[q->tail] = item;
    q->tail = (q->tail +1) % QUEUE_CAPACITY;

    if((err = pthread_mutex_unlock(&q->mutex)))
        exit_with_sys_msg("mutex unlock",err);

    if(sem_post(&q->full_slots))
        exit_with_sys("full_slots wait");

}

void queue_pop(safe_queue_t *q, queue_item_t *item)
{

    assert(q != NULL);
    int err;

    if(sem_wait(&q->full_slots))
        exit_with_sys("full_slots wait");

    if((err = pthread_mutex_lock(&q->mutex)))
        exit_with_sys_msg("mutex lock",err);

    *item = q->items[q->head];
    q->head = (q->head +1) % QUEUE_CAPACITY;

    if((err = pthread_mutex_unlock(&q->mutex)))
        exit_with_sys_msg("mutex unlock",err);

    if(sem_post(&q->empty_slots))
        exit_with_sys("empty_slots post");
}

void *reader_thread(void *arg)
{
    assert(arg != NULL);
    reader_arg_t *r_arg = (reader_arg_t *)arg;
    shared_context_t *shared = r_arg->shared;
    unsigned int file_idx = r_arg->file_idx;
    const char *filename = shared->filenames[file_idx];

    int fd = open(filename, O_RDONLY);
    if(fd == -1)
        exit_with_sys("open");
    
    struct stat sb;
    if(fstat(fd, &sb) == -1)
        exit_with_sys("fstat");

    size_t filesize = sb.st_size;
    if(filesize < 4)
    {
        fprintf(stderr, "File %s non valido (dimensione <4)\n", filename);
        close(fd);
        exit(EXIT_FAILURE);
    }


    unsigned long total_vectors = (filesize -4) / 16;
    shared->stats[file_idx].totali = total_vectors;

    uint8_t *mapped = mmap(NULL, filesize, PROT_READ, MAP_PRIVATE, fd, 0);
    if(mapped == MAP_FAILED)
        exit_with_sys("mmap");

    uint32_t dichiarati = *(uint32_t *) mapped;
    shared->stats[file_idx].dichiarati = dichiarati;

    printf("[READER-%u] file '%s': %lu vettori totali, %u dichiarati equisomma\n",
            file_idx + 1, filename, total_vectors, dichiarati);
    
    for(unsigned long i=0; i<total_vectors; i++)
    {
        unsigned long offset = 4 + (i * 16);

        printf("[READER-%u] vettore candidato n.%lu (offset %lu)\n",
                file_idx + 1, i + 1, offset);
        
        queue_item_t item;
        memcpy(item.vec, mapped + offset, 8 * sizeof(int16_t));
        item.file_id = file_idx;
        item.vector_num = i +1;
        item.is_poison_pill = false;

        queue_push(&shared->queue, item);
    }

    if(munmap(mapped, filesize) == -1)
        exit_with_sys("munmap");

    close(fd);

    printf("[READER-%u] terminazione con %lu vettori letti\n",
            file_idx + 1, total_vectors);

    return NULL;

    
}

void *verifier_thread(void *arg)
{
    assert(arg != NULL);
    verifier_arg_t *v_arg = (verifier_arg_t *) arg;
    shared_context_t *shared = v_arg->shared;
    unsigned int v_id = v_arg->verifier_id;

    unsigned long verified_count = 0;
    int err;

    while(true)
    {
        queue_item_t item;
        queue_pop(&shared->queue, &item);

        if(item.is_poison_pill)
            break;

        verified_count++;

        int somma_pari = item.vec[0]  + item.vec[2] + item.vec[4] + item.vec[6];
        int somma_dispari = item.vec[1] + item.vec[3] + item.vec[5] + item.vec[7];
        bool is_equisomma = (somma_pari == somma_dispari);
        
        flockfile(stdout);
        printf("[VERIF-%u] vettore n.%lu del file '%s': (",
                v_id, item.vector_num, shared->filenames[item.file_id]);
        for(int k=0; k<8; k++)
        {
            printf("%d%s", item.vec[k], (k < 7 ? ", " : ""));
        }
        printf(") somma pari %d, somma dispari %d -> %s\n",
                somma_pari, somma_dispari, is_equisomma ? "equisomma" : "non equisomma");
        funlockfile(stdout);

        if((err = pthread_mutex_lock(&shared->stast_lock)))
            exit_with_sys_msg("pthread_mutex_lock", err);
        
        if(is_equisomma)
            shared->stats[item.file_id].equisomma++;
        else
            shared->stats[item.file_id].non_equisomma++;

         if((err = pthread_mutex_unlock(&shared->stast_lock)))
            exit_with_sys_msg("pthread_mutex_unlock", err);
    }

    printf("[VERIF-%u] terminazione con %lu vettori verificati\n",
           v_id, verified_count);

    return NULL;
}


int main(int argc, char *argv[])
{
    if(argc < 3)
    {
        fprintf(stderr, "Uso: %s <M-verifiers> <file-bin-1> [file-bin-2 ...]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

   int M = atoi(argv[1]); 
   if(M<= 0)
   {
        fprintf(stderr,"M-verifiers deve essere > 0\n");
        exit(EXIT_FAILURE);
   }

   unsigned int N = argc - 2;
   char **file_list = &argv[2];

   
   printf("[MAIN] creazione di %u thread lettori e %d thread verificatori\n", N, M);
 
   shared_context_t shared;
   shared.num_files = N;
   shared.num_verifiers = M;
   shared.filenames = file_list;
   shared.stats = calloc(N, sizeof(file_stats_t));
   

   if(shared.stats == NULL)
    exit_with_sys("calloc stats");

   int err;
   
   if((err = pthread_mutex_init(&shared.stast_lock, NULL)))
    exit_with_sys_msg("pthread_mutex_init stats", err);

    queue_init(&shared.queue);

    pthread_t reader_tids[N];
    reader_arg_t reader_args[N];
    for(unsigned int i=0; i<N; i++)
    {
        reader_args[i].file_idx = i;
        reader_args[i].shared = &shared;

        if((err = pthread_create(&reader_tids[i], NULL, reader_thread, &reader_args[i])))
            exit_with_sys_msg("pthread_create reader", err);
    }


    pthread_t verifier_tids[M];
    verifier_arg_t verifier_args[M];
    for(unsigned int j=0; j<M; j++)
    {
        verifier_args[j].verifier_id = j + 1;
        verifier_args[j].shared = &shared;

        if((err = pthread_create(&verifier_tids[j], NULL, verifier_thread, &verifier_args[j])))
            exit_with_sys_msg("pthread_create verifier", err);
        }


    for(unsigned int i=0; i<N; i++)
    {
        if((err = pthread_join(reader_tids[i], NULL)))
           exit_with_sys_msg("pthread_join reader", err); 
    }

    queue_item_t pill;
    memset(&pill, 0, sizeof(pill));
    pill.is_poison_pill = true;
    for (int j = 0; j < M; j++) {
        queue_push(&shared.queue, pill);
    }

    for(unsigned int j=0; j<M; j++)
    {
        if((err = pthread_join(verifier_tids[j], NULL)))
           exit_with_sys_msg("pthread_join verifier", err); 
    }

    unsigned int superate = 0;
    for(unsigned int i=0; i<N; i++)
    {
        file_stats_t *s = &shared.stats[i];
        const char *fn = shared.filenames[i];
        double perc = (s->totali >0) ? ((double)s->equisomma / s->totali) * 100.0 : 0.0;
        
        if(s->equisomma == s->dichiarati)
        {
            superate++;
            printf("[MAIN] file '%s': %lu equisomma e %lu non equisomma su %lu vettori (%.1f%%) -> verifica automatica superata (dichiarati %u)\n",
                   fn, s->equisomma, s->non_equisomma, s->totali, perc, s->dichiarati);
        }
        else
        {
            printf("[MAIN] file '%s': %lu equisomma e %lu non equisomma su %lu vettori (%.1f%%) -> verifica automatica fallita (dichiarati %u, rilevati %lu)\n",
                   fn, s->equisomma, s->non_equisomma, s->totali, perc, s->dichiarati, s->equisomma);
        }
    }

    printf("[MAIN] verifiche superate: %u/%u\n", superate, N);
    printf("[MAIN] terminazione\n");

    queue_destroy(&shared.queue);
    pthread_mutex_destroy(&shared.stast_lock);
    free(shared.stats);

    return 0;
}