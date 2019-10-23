# Compiler and Compile options.
CC = g++

# Macros specifying path for compile.
SRCS := $(wildcard src/*.cpp)
INCS := $(wildcard include/*.h)
INC = ./include/

TARGET = run
override CFLAGS += -Wall -g -O2 -std=c++14 -I$(INC) -lpthread

all: $(TARGET)

$(TARGET): $(SRCS) $(INCS)
	$(CC) -o $(TARGET) $(SRCS) $(CFLAGS)

# Delete binary & object files
clean:
	$(RM) $(TARGET) $(OBJS)
	$(RM) thread*.txt
