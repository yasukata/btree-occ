PROGS = btbench

CLEANFILES = $(PROGS) *.o

SRCDIR ?= ./

CC=g++

NO_MAN=
CPPFLAGS = -O3 -pipe -g
CPPFLAGS += -Werror -Wall -Wunused-function
CPPFLAGS += -Wextra
CPPFLAGS += -I$(SRCDIR)
CPPFLAGS += -std=c++11

LDLIBS += -lpthread

SRCS = bench.cpp btree.cpp
OBJS += $(SRCS:.cpp=.o)
OBJS += mt19937ar.o

.PHONY: all
all: $(PROGS)

$(PROGS): $(OBJS)
	$(CC) $(CPPFLAGS) $(LDFLAGS) $^ -o $@ $(LDLIBS)

clean:
	-@rm -rf $(CLEANFILES)
