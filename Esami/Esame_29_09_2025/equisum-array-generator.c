#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <semaphore.h>
#include <pthread.h>
#include <time.h>
#include <stdbool.h>
#include <errno.h>

#define QUEUE_CAPACITY 5

typedef struct
{
    int *vec;
    int round;
    bool poison_pill;

}queue_item_t;

typedef struct
{
    queue_item_t item[QUEUE_CAPACITY];
    unsigned int head;
    unsigned int tail;

    sem_t empty_slots;
    sem_t full_slots;
    pthread_mutex_t mutex;


}safe_queue_t;

typedef struct
{
    safe_queue_t queue_proposte;
    safe_queue_t queue_scartati;

    unsigned int N;
    unsigned int T;
    unsigned int R;

}shared_context_t;

typedef struct
{
    unsigned int id;
    shared_context_t *shared;

}repairer_arg_t;

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


void queue_init(safe_queue_t *q)
{
    assert(q!= NULL);
    int err;

    q->head = 0;
    q->tail = 0;

    if((err = pthread_mutex_init(&q->mutex, NULL)))
        exit_with_err("mutex_init", err);

    if(sem_init(&q->empty_slots, 0, QUEUE_CAPACITY))
        exit_with_sys("sem_init empty_slots");

    if(sem_init(&q->full_slots, 0, 0))
        exit_with_sys("sem_init full_slots");
}

void queue_destroy(safe_queue_t *q)
{
    assert(q!= NULL);
    int err;

    if((err = pthread_mutex_destroy(&q->mutex)))
        exit_with_err("mutex_destroy", err);

    if(sem_destroy(&q->empty_slots))
        exit_with_sys("sem_destroy empty_slots");

    if(sem_destroy(&q->full_slots))
        exit_with_sys("sem_destroy full_slots");
}

void queue_push(safe_queue_t *q, queue_item_t item)
{
    assert(q!= NULL);
    int err;
    
    if(sem_wait(&q->empty_slots))
        exit_with_sys("sem_wait empty_slots");

    if((err = pthread_mutex_lock(&q->mutex)))
        exit_with_err("mutex_lock", err);

    q->item[q->tail] = item;
    q->tail = (q->tail + 1) % QUEUE_CAPACITY;

    if((err = pthread_mutex_unlock(&q->mutex)))
        exit_with_err("mutex_unlock", err);

     if(sem_post(&q->full_slots))
        exit_with_sys("sem_signal full_slots");
}


void queue_pop(safe_queue_t *q, queue_item_t *item)
{
    assert(q!= NULL);
    assert(item != NULL);
    int err;
    
    if(sem_wait(&q->full_slots))
        exit_with_sys("sem_wait full_slots");

    if((err = pthread_mutex_lock(&q->mutex)))
        exit_with_err("mutex_lock", err);

    *item = q->item[q->head];
    q->head = (q->head + 1) % QUEUE_CAPACITY;

    if((err = pthread_mutex_unlock(&q->mutex)))
        exit_with_err("mutex_unlock", err);

     if(sem_post(&q->empty_slots))
        exit_with_sys("sem_signal empty_slots");
}

void *generator_thread(void *arg)
{
    assert(arg != NULL);

    shared_context_t *shared = (shared_context_t *) arg;
    
    unsigned int seed = (unsigned int) time(NULL);
    queue_item_t item;

    memset(&item,0,sizeof(item));

    for(unsigned int i=0; i<shared->T; i++)
    {

        item.vec = malloc( shared->N * sizeof(int));
        if(item.vec == NULL)
            exit_with_sys("malloc");

        for(unsigned int j=0; j<shared->N; j++)
        {
            item.vec[j] = rand_r(&seed) % 100;
        }

        flockfile(stdout);
        printf("[GEN] vettore candidato n.%u: ", i+1);
        for(unsigned int j=0; j<shared->N; j++)
        {
            printf("%d%s",item.vec[j], j<shared->N-1 ? ", " : "\n");
        }
        funlockfile(stdout);

        item.round = 0;
        item.poison_pill = false;

        queue_push(&shared->queue_proposte, item);
    }

    return NULL;
}

void *verifier_thread(void *arg)
{
    assert(arg != NULL);
    shared_context_t *shared = (shared_context_t *) arg;

    queue_item_t item;

    unsigned int accepted_count = 0;
    unsigned int finished_repairers = 0;

    while(true)
    {
        queue_pop(&shared->queue_proposte,&item);

        unsigned int sum_pari = 0;
        unsigned int sum_dispari = 0;

        for(unsigned int i=0; i<shared->N; i++)
        {
            if(i % 2 == 0)
                sum_pari += item.vec[i];
            else
                sum_dispari +=item.vec[i];
        }

        flockfile(stdout);
        printf("[VER] estratto un vettore candidato con round pari a %d: ", item.round);

        for(unsigned int i=0; i<shared->N; i++)
        {
            printf("%d%s",item.vec[i], i<shared->N-1 ? ", " : "\n");
        }

        if(sum_pari == sum_dispari)
        {
            accepted_count++;
            printf("[VER] vettore verificato e accettato dopo %d round di aggiustamenti (%u di %u)\n",
                item.round, accepted_count, shared->T);
            funlockfile(stdout);

            free(item.vec);

            if(accepted_count == shared->T)
                break;
        }
        else
        {
            printf("[VER] vettore non verificato e rigettato\n");
            funlockfile(stdout);
            queue_push(&shared->queue_scartati, item);
        }
    }

    printf("[VER] terminato con %u vettori equisomma trovati\n", accepted_count);

    queue_item_t pill;
    memset(&pill, 0, sizeof(pill));
    pill.poison_pill = true;

    for(unsigned int i=0; i<shared->R; i++)
    {
        queue_push(&shared->queue_scartati, pill);
    }


    // evità il deadlock dell'anello
    while(true)
    {
        queue_pop(&shared->queue_proposte, &item);

        if(item.poison_pill)
            finished_repairers++;
        else
            free(item.vec);

        if(finished_repairers == shared->R)
            break;

    }

    return NULL;
}

void *repairer_thread(void *arg)
{
    assert(arg != NULL);
    repairer_arg_t *r_arg = (repairer_arg_t *) arg;
    shared_context_t *shared = r_arg->shared;
    unsigned int r_id = r_arg->id;

    queue_item_t item;

    unsigned int seed = (unsigned int) time(NULL)  ^ r_id;

    while(true)
    {
        queue_pop(&shared->queue_scartati,&item);

        if(item.poison_pill)
            break;
            
        flockfile(stdout);
        printf("[REP-%u] estratto un vettore da riparare: ", r_id);
        for(unsigned int i=0; i<shared->N; i++)
        {
            printf("%d%s",item.vec[i], i<shared->N-1 ? ", " : "\n");
        }
        funlockfile(stdout);

        item.round++;
        unsigned int idx = rand_r(&seed) % shared->N;
        int old_val = item.vec[idx];
        int new_val = rand_r(&seed) % 100;
        item.vec[idx] = new_val;

        flockfile(stdout);
        printf("[REP-%u] reinserito vettore riparato (%d->%d) con round pari a %d: ",
            r_id, old_val, new_val, item.round);

        for(unsigned int i=0; i<shared->N; i++)
        {
            printf("%d%s",item.vec[i], i<shared->N-1 ? ", " : "\n");
        }
        funlockfile(stdout);

        queue_push(&shared->queue_proposte, item);
    }

    queue_push(&shared->queue_proposte, item);

    printf("[REP-%u] terminato\n", r_id);

    return NULL;
}

int main(int argc, char *argv[])
{
    if(argc != 4)
    {
        fprintf(stderr,"Errore: riga di comando -> %s <N> <T> <R>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int N = atoi(argv[1]);
    int T = atoi(argv[2]);
    int R = atoi(argv[3]);

    int err;

    if(N < 2 ||T <= 0 || R <= 0)
        exit_with_err("Parametri non validi", EINVAL);

    shared_context_t shared;
    memset(&shared, 0, sizeof(shared));

    shared.N = (unsigned int) N;
    shared.T = (unsigned int) T;
    shared.R = (unsigned int) R;

    queue_init(&shared.queue_proposte);
    queue_init(&shared.queue_scartati);

    printf("[MAIN] creazione di un thread generator, di un thread verificatore e di %d thread riparatori\n",R);


    pthread_t generator;
    if((err = pthread_create(&generator, NULL, generator_thread, &shared)))
        exit_with_err("pthread_create generator", err);


    pthread_t verifier;
    if((err = pthread_create(&verifier, NULL, verifier_thread, &shared)))
        exit_with_err("pthread_create verifier", err);

    pthread_t repairer[shared.R];
    repairer_arg_t r_arg[shared.R];

    for(unsigned int i=0; i<shared.R; i++)
    {
        r_arg[i].id = i+1;
        r_arg[i].shared = &shared;

        if((err = pthread_create(&repairer[i], NULL, repairer_thread, &r_arg[i])))
            exit_with_err("pthread_create repairer", err);
    }

    if((err = pthread_join(generator,NULL)))
        exit_with_err("pthread_join generator",err);

    if((err = pthread_join(verifier,NULL)))
        exit_with_err("pthread_join verifier",err);

    for(unsigned int i=0; i<shared.R; i++)
    {
        if((err = pthread_join(repairer[i], NULL)))
            exit_with_err("pthread_join repairer", err);
    }

    queue_destroy(&shared.queue_proposte);
    queue_destroy(&shared.queue_scartati);

    printf("[MAIN] terminato\n");

    return 0;


}