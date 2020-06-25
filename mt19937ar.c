/* 
   A C-program for MT19937, with initialization improved 2002/1/26.
   Coded by Takuji Nishimura and Makoto Matsumoto.

   Before using, initialize the state by using init_genrand(seed)  
   or init_by_array(init_key, key_length).

   Copyright (C) 1997 - 2002, Makoto Matsumoto and Takuji Nishimura,
   All rights reserved.                          

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions
   are met:

     1. Redistributions of source code must retain the above copyright
        notice, this list of conditions and the following disclaimer.

     2. Redistributions in binary form must reproduce the above copyright
        notice, this list of conditions and the following disclaimer in the
        documentation and/or other materials provided with the distribution.

     3. The names of its contributors may not be used to endorse or promote 
        products derived from this software without specific prior written 
        permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
   EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
   PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


   Any feedback is very welcome.
   http://www.math.sci.hiroshima-u.ac.jp/~m-mat/MT/emt.html
   email: m-mat @ math.sci.hiroshima-u.ac.jp (remove space)
*/

#include <stdio.h>

#include "mt19937ar.h"

/* Period parameters */  
#define M 397
#define MATRIX_A 0x9908b0dfUL   /* constant vector a */
#define UPPER_MASK 0x80000000UL /* most significant w-r bits */
#define LOWER_MASK 0x7fffffffUL /* least significant r bits */

/* initializes _m->mt[N624] with a seed */
void init_genrand(struct imt *_m, unsigned long s)
{
    _m->mti = N624 + 1;
    _m->mt[0]= s & 0xffffffffUL;
    for (_m->mti=1; _m->mti<N624; _m->mti++) {
        _m->mt[_m->mti] = 
	    (1812433253UL * (_m->mt[_m->mti-1] ^ (_m->mt[_m->mti-1] >> 30)) + _m->mti); 
        /* See Knuth TAOCP Vol2. 3rd Ed. P.106 for multiplier. */
        /* In the previous versions, MSBs of the seed affect   */
        /* only MSBs of the array _m->mt[].                        */
        /* 2002/01/09 modified by Makoto Matsumoto             */
        _m->mt[_m->mti] &= 0xffffffffUL;
        /* for >32 bit machines */
    }
}

/* initialize by an array with array-length */
/* init_key is the array for initializing keys */
/* key_length is its length */
/* slight change for C++, 2004/2/26 */
void init_by_array(struct imt *_m, unsigned long init_key[], int key_length)
{
    int i, j, k;
    init_genrand(_m, 19650218UL);
    i=1; j=0;
    k = (N624>key_length ? N624 : key_length);
    for (; k; k--) {
        _m->mt[i] = (_m->mt[i] ^ ((_m->mt[i-1] ^ (_m->mt[i-1] >> 30)) * 1664525UL))
          + init_key[j] + j; /* non linear */
        _m->mt[i] &= 0xffffffffUL; /* for WORDSIZE > 32 machines */
        i++; j++;
        if (i>=N624) { _m->mt[0] = _m->mt[N624-1]; i=1; }
        if (j>=key_length) j=0;
    }
    for (k=N624-1; k; k--) {
        _m->mt[i] = (_m->mt[i] ^ ((_m->mt[i-1] ^ (_m->mt[i-1] >> 30)) * 1566083941UL))
          - i; /* non linear */
        _m->mt[i] &= 0xffffffffUL; /* for WORDSIZE > 32 machines */
        i++;
        if (i>=N624) { _m->mt[0] = _m->mt[N624-1]; i=1; }
    }

    _m->mt[0] = 0x80000000UL; /* MSB is 1; assuring non-zero initial array */ 
}

/* generates a random number on [0,0xffffffff]-interval */
unsigned long genrand_int32(struct imt *_m)
{
    unsigned long y;
    static unsigned long mag01[2]={0x0UL, MATRIX_A};
    /* mag01[x] = x * MATRIX_A  for x=0,1 */

    if (_m->mti >= N624) { /* generate N624 words at one time */
        int kk;

        if (_m->mti == N624+1)   /* if init_genrand() has not been called, */
            init_genrand(_m, 5489UL); /* a default initial seed is used */

        for (kk=0;kk<N624-M;kk++) {
            y = (_m->mt[kk]&UPPER_MASK)|(_m->mt[kk+1]&LOWER_MASK);
            _m->mt[kk] = _m->mt[kk+M] ^ (y >> 1) ^ mag01[y & 0x1UL];
        }
        for (;kk<N624-1;kk++) {
            y = (_m->mt[kk]&UPPER_MASK)|(_m->mt[kk+1]&LOWER_MASK);
            _m->mt[kk] = _m->mt[kk+(M-N624)] ^ (y >> 1) ^ mag01[y & 0x1UL];
        }
        y = (_m->mt[N624-1]&UPPER_MASK)|(_m->mt[0]&LOWER_MASK);
        _m->mt[N624-1] = _m->mt[M-1] ^ (y >> 1) ^ mag01[y & 0x1UL];

        _m->mti = 0;
    }
  
    y = _m->mt[_m->mti++];

    /* Tempering */
    y ^= (y >> 11);
    y ^= (y << 7) & 0x9d2c5680UL;
    y ^= (y << 15) & 0xefc60000UL;
    y ^= (y >> 18);

    return y;
}

/* generates a random number on [0,0x7fffffff]-interval */
long genrand_int31(struct imt *_m)
{
    return (long)(genrand_int32(_m)>>1);
}

/* generates a random number on [0,1]-real-interval */
double genrand_real1(struct imt *_m)
{
    return genrand_int32(_m)*(1.0/4294967295.0); 
    /* divided by 2^32-1 */ 
}

/* generates a random number on [0,1)-real-interval */
double genrand_real2(struct imt *_m)
{
    return genrand_int32(_m)*(1.0/4294967296.0); 
    /* divided by 2^32 */
}

/* generates a random number on (0,1)-real-interval */
double genrand_real3(struct imt *_m)
{
    return (((double)genrand_int32(_m)) + 0.5)*(1.0/4294967296.0); 
    /* divided by 2^32 */
}

/* generates a random number on [0,1) with 53-bit resolution*/
double genrand_res53(struct imt *_m)
{ 
    unsigned long a=genrand_int32(_m)>>5, b=genrand_int32(_m)>>6; 
    return(a*67108864.0+b)*(1.0/9007199254740992.0); 
} 
/* These real versions are due to Isaku Wada, 2002/01/09 added */
#if 0
int main(void)
{
    int i;
    unsigned long init[4]={0x123, 0x234, 0x345, 0x456}, length=4;
    init_by_array(init, length);
    printf("1000 outputs of genrand_int32()\n");
    for (i=0; i<1000; i++) {
      printf("%10lu ", genrand_int32());
      if (i%5==4) printf("\n");
    }
    printf("\n1000 outputs of genrand_real2()\n");
    for (i=0; i<1000; i++) {
      printf("%10.8f ", genrand_real2());
      if (i%5==4) printf("\n");
    }
    return 0;
}
#endif
