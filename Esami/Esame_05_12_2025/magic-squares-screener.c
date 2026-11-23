#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <assert.h>
#include <semaphore.h>


#define QUEUE_INTERMEDIATE 10
#define QUEUE_FINAL 3

typedef struct
{
    int square[9];
    unsigned int id_square;
    int magic_total;
    bool is_poison_pill;

}queue_item_t;


typedef struct
{
    queue_item_t *items;
    unsigned int head;
    unsigned int tail;
    unsigned int capacity;

    pthread_mutex_t mutex;
    sem_t empty_slots;
    sem_t full_slots;

}safe_queue_t;

typedef struct
{
    safe_queue_t queue_mid;
    safe_queue_t queue_final;

    unsigned int num_verifiers;

    unsigned int active_readers;
    pthread_mutex_t readers_mutex;

    unsigned int active_verifier;
    pthread_mutex_t verifier_mutex;

}shared_context_t;

typedef struct
{
    unsigned int reader_id;
    shared_context_t *shared;
    const char *filename;

}reader_arg_t;

typedef struct
{
    unsigned int verifier_id;
    shared_context_t *shared;

}verifier_arg_t;

void exit_with_sys(const char *s)
{
    perror(s);
    exit(EXIT_FAILURE);
}

void exit_with_err(const char *s, int e)
{
    fprintf(stderr, "%s: %s\n", s, strerror(e));
    exit(EXIT_FAILURE);
}

void queue_init(safe_queue_t *q, int capacity)
{
    int err;
    assert(q != NULL);

    q->head = 0;
    q->tail = 0;
    q->capacity = capacity;

    q->items = malloc(q->capacity * sizeof(queue_item_t));
    
    if(q->items == NULL)
        exit_with_sys("malloc");

    if((err = pthread_mutex_init(&q->mutex, NULL)))
        exit_with_err("mutex_init", err);

    if(sem_init(&q->empty_slots, 0, q->capacity))
        exit_with_sys("sem_init empty_slots");

    if(sem_init(&q->full_slots, 0, 0))
        exit_with_sys("sem_init full_slots");
}

void queue_destroy(safe_queue_t *q)
{
    int err;
    assert(q != NULL);

    if((err = pthread_mutex_destroy(&q->mutex)))
        exit_with_err("mutex_destroy", err);

    if(sem_destroy(&q->empty_slots))
        exit_with_sys("sem_destroy empty_slots");

    if(sem_destroy(&q->full_slots))
        exit_with_sys("sem_destroy full_slots");

    free(q->items);
}

void queue_push(safe_queue_t *q, queue_item_t item)
{
    assert(q != NULL);
    int err;

    if(sem_wait(&q->empty_slots))
        exit_with_sys("sem_wait empty_slots");

    if((err = pthread_mutex_lock(&q->mutex)))
        exit_with_err("mutex_lock",err);

    q->items[q->tail] = item;
    q->tail = (q->tail + 1) % q->capacity;

    if((err = pthread_mutex_unlock(&q->mutex)))
        exit_with_err("mutex_unlock",err);

    if(sem_post(&q->full_slots))
        exit_with_sys("sem_post full_slots");

}

void queue_pop(safe_queue_t *q, queue_item_t *item)
{
    assert(q != NULL);
    int err;

    if(sem_wait(&q->full_slots))
        exit_with_sys("sem_wait full_slots");

    if((err = pthread_mutex_lock(&q->mutex)))
        exit_with_err("mutex lock",err);

    *item = q->items[q->head];
    q->head = (q->head + 1) % q->capacity;

    if((err = pthread_mutex_unlock(&q->mutex)))
        exit_with_err("mutex_unlock",err);

    if(sem_post(&q->empty_slots))
        exit_with_sys("sem_post empty_slots");

}

void *reader_thread(void *arg)
{
    assert(arg != NULL);
    reader_arg_t *r_arg = (reader_arg_t *) arg;
    shared_context_t *shared = r_arg->shared;
    unsigned int r_id = r_arg->reader_id;

    int err;

    FILE *f = fopen(r_arg->filename,"r");

    if(f==NULL)
        exit_with_sys("fopen");
    
    char line[256];
    unsigned int count = 0;

    printf("[READER-%u] file '%s'\n", r_id, r_arg->filename);

    while(fgets(line,sizeof(line),f) != NULL)
    {
        queue_item_t item;
        memset(&item,0, sizeof(item));
        
        if(sscanf(line,"%d,%d,%d,%d,%d,%d,%d,%d,%d",
            &item.square[0],&item.square[1],&item.square[2],
            &item.square[3],&item.square[4],&item.square[5],
            &item.square[6],&item.square[7],&item.square[8]) == 9)
        {
            item.id_square = ++count;
        
        printf("[READER-%u] quadrato candidato n.%u: (%d, %d, %d) (%d, %d, %d) (%d, %d, %d)\n",
                r_id, item.id_square,
                item.square[0],item.square[1],item.square[2],
                item.square[3],item.square[4],item.square[5],
                item.square[6],item.square[7],item.square[8]);

            queue_push(&shared->queue_mid, item);
        }
        
    }

    fclose(f);

    if((err = pthread_mutex_lock(&shared->readers_mutex)))
        exit_with_err("mutex_lock readers",err);
    
    shared->active_readers--;

    if(shared->active_readers == 0)
    {
        queue_item_t pill;
        memset(&pill, 0, sizeof(pill));
        pill.is_poison_pill = true;

        for(unsigned int i=0; i< shared->num_verifiers; i++)
        {
            queue_push(&shared->queue_mid, pill);
        }
    }

    if((err = pthread_mutex_unlock(&shared->readers_mutex)))
        exit_with_err("mutex_unlock readers",err);

    printf("[READER-%u] terminazione\n", r_id);

    return NULL;
}

void *verifier_thread(void *arg)
{
    assert(arg != NULL);
    verifier_arg_t *v_arg = (verifier_arg_t *) arg;
    shared_context_t *shared = v_arg->shared;
    unsigned int v_id = v_arg->verifier_id;
    int err;

    queue_item_t item;
    
    while(true)
    {
        queue_pop(&shared->queue_mid,&item);

        if(item.is_poison_pill)
            break;

        printf("[VERIF-%u] verifico quadrato: (%d, %d, %d) (%d, %d, %d) (%d, %d, %d)\n",
            v_id, item.square[0], item.square[1],
            item.square[2], item.square[3], item.square[4],
            item.square[5], item.square[6], item.square[7],
            item.square[8]);

        int target = item.square[0] + item.square[1] + item.square[2];

        if ((item.square[3] + item.square[4] + item.square[5] == target) && // riga 2
        (item.square[6] + item.square[7] + item.square[8] == target) && // riga 3
        (item.square[0] + item.square[3] + item.square[6] == target) && // col 1
        (item.square[1] + item.square[4] + item.square[7] == target) && // col 2
        (item.square[2] + item.square[5] + item.square[8] == target) && // col 3
        (item.square[0] + item.square[4] + item.square[8] == target) && // diag 1
        (item.square[2] + item.square[4] + item.square[6] == target))
        {
            item.magic_total = target;
            printf("[VERIF-%u] trovato quadrato magico!\n", v_id);
            queue_push(&shared->queue_final,item);
        }
        
    }

    if((err = pthread_mutex_lock(&shared->verifier_mutex)))
        exit_with_err("mutex_lock verifier", err);

    shared->active_verifier--;

    if(shared->active_verifier == 0)
    {
        queue_item_t pill;
        memset(&pill, 0, sizeof(pill));
        pill.is_poison_pill = true;
        queue_push(&shared->queue_final, pill);
    }

    if((err = pthread_mutex_unlock(&shared->verifier_mutex)))
        exit_with_err("mutex_unlock verifier", err);

    printf("[VERIF-%u] terminazione\n", v_id);

    return NULL;
}

int main(int argc, char *argv[])
{
    if(argc < 3)
    {
        fprintf(stderr,"Uso: %s <M-verifiers> <file-1> ...  <file-N>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    if(atoi(argv[1]) <= 0)
    {
        fprintf(stderr,"<M-verifiers> > 0\n");
        exit(EXIT_FAILURE);
    }

    unsigned int M = (unsigned int) atoi(argv[1]);


    unsigned int N = argc - 2;
    char **file_list = &argv[2];

    printf("[MAIN] creazione di %u thread lettori e %u thread verificatori\n", N, M);

    shared_context_t shared;
    memset(&shared, 0, sizeof(shared));
    shared.num_verifiers = M;
    

    int err;
    queue_item_t item;

    queue_init(&shared.queue_mid, QUEUE_INTERMEDIATE);
    queue_init(&shared.queue_final, QUEUE_FINAL);

    shared.active_readers = N;
    shared.active_verifier = M;

    if((err = pthread_mutex_init(&shared.readers_mutex,NULL)))
        exit_with_err("mutex_init shared.readers", err);

    if((err =pthread_mutex_init(&shared.verifier_mutex,NULL)))
        exit_with_err("mutex_init shared.verifier", err);


    pthread_t reader_tids[N];
    reader_arg_t reader_args[N];

    for(unsigned int i=0; i<N; i++)
    {
        reader_args[i].reader_id = i + 1;
        reader_args[i].filename = file_list[i];
        reader_args[i].shared = &shared;

        if((err = pthread_create(&reader_tids[i], NULL, reader_thread, &reader_args[i])))
            exit_with_err("pthread_create reader_tids",err);
    }

    pthread_t verifier_tids[M];
    verifier_arg_t verifier_args[M];

    for(unsigned int j=0; j<M; j++)
    {
        verifier_args[j].verifier_id = j + 1;
        verifier_args[j].shared = &shared;

        if((err = pthread_create(&verifier_tids[j], NULL, verifier_thread, &verifier_args[j])))
            exit_with_err("pthread_create verifier_tids",err);
    }

    while(true)
    {
        queue_pop(&shared.queue_final, &item);

        if(item.is_poison_pill)
            break;
        else
        {
            flockfile(stdout);
            printf("[MAIN] quadrato magico trovato:\n(%d, %d, %d)\n(%d, %d, %d)\n(%d, %d, %d)\ntotale %d\n",
                item.square[0], item.square[1], item.square[2],
                item.square[3], item.square[4], item.square[5],
                item.square[6], item.square[7], item.square[8],
                item.magic_total);
            funlockfile(stdout);
        }
    }

    for(unsigned int i=0; i<N; i++)
    {
        if((err = pthread_join(reader_tids[i],NULL)))
            exit_with_err("pthread_join reader_tids",err);
    }

    for(unsigned int j=0; j<M; j++)
    {
        if((err = pthread_join(verifier_tids[j],NULL)))
            exit_with_err("pthread_join verifier_tids",err);
    }

    queue_destroy(&shared.queue_mid);
    queue_destroy(&shared.queue_final);

    if((err = pthread_mutex_destroy(&shared.readers_mutex)))
        exit_with_err("mutex_destroy shared.readers", err);

    if((err =pthread_mutex_destroy(&shared.verifier_mutex)))
        exit_with_err("mutex_destroy shared.verifier", err);
    

    printf("[MAIN] terminazione\n");

    return 0;
    
}