# Compiler and Compile options.
CC = g++

# Macros specifying path for compile.
SRCS := $(wildcard src/*.cpp)
INCS := $(wildcard include/*.h)
INC = ./include/

TARGET = run
override CFLAGS += -g -std=c++17 -I$(INC) -lpthread

all: $(TARGET) $(VALIDATE)

$(TARGET): $(SRCS) $(INCS)
	$(CC) -o $@ $(SRCS) $(CFLAGS)

# Delete binary & object files
clean:
	$(RM) $(TARGET) $(VALIDATE)

test:
	make all
	./run 1
	./run 1
	./run 1
	./run 2
	./run 2
	./run 2
	./run 4
	./run 4
	./run 4
	./run 8
	./run 8
	./run 8
	./run 16
	./run 16
	./run 16
	./run 32
	./run 32
	./run 32
	./run 64
	./run 64
	./run 64
	./run 128
	./run 128
	./run 128
