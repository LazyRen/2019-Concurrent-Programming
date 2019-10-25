# Compiler and Compile options.
CC = g++

# Macros specifying path for compile.
SRCS := $(wildcard src/*.cpp)
INCS := $(wildcard include/*.h)
INC = ./include/

TARGET = run
VALIDATE = validation
override CFLAGS += -Wall -g -O2 -std=c++14 -I$(INC) -lpthread

all: $(TARGET) $(VALIDATE)

$(TARGET): $(SRCS) $(INCS)
	$(CC) -o $@ $(SRCS) $(CFLAGS)

$(VALIDATE): $(VALIDATE).cpp
	$(CC) -o $@ $^ $(CFLAGS)

# Delete binary & object files
clean:
	$(RM) $(TARGET) $(VALIDATE)
	$(RM) thread*.txt
