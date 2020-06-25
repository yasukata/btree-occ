#ifndef _MT19937AR_H
#define _MT19937AR_H

#define N624 624

struct imt {
    unsigned long mt[N624]; /* the array for the state vector  */
    int mti; /* mti==N+1 means mt[N] is not initialized */
};

void init_genrand(struct imt *_m, unsigned long s);
unsigned long genrand_int32(struct imt *_m);

#endif
