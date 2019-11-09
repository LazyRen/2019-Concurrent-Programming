# Compiler and Compile options.
CC = g++

# Macros specifying path for compile.
SRCS := $(wildcard src/*.cpp)
INCS := $(wildcard include/*.h)
INC = ./include/

TARGET = run
override CFLAGS += -Wall -g -std=c++17 -I$(INC) -lpthread

all: $(TARGET) $(VALIDATE)

$(TARGET): $(SRCS) $(INCS)
	$(CC) -o $@ $(SRCS) $(CFLAGS)

# Delete binary & object files
clean:
	$(RM) $(TARGET) $(VALIDATE)
