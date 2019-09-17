# Compiler and Compile options.
CC = g++
CFLAGS = -Wall -g -O2

# Macros specifying path for compile.
SRCS := $(wildcard src/*.cpp)
OBJS := $(SRCS:.c=.o)
INC = ./include/
LIB = ./lib/

# Pre-Processor.
CFLAGS += -std=c++14 -I$(INC) -L$(LIB) -lpthread


TARGET = run


all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) -o $(TARGET) $(OBJS) $(CFLAGS)



# Delete binary & object files
clean:
	$(RM) $(TARGET) $(OBJS)

test:
	make
	time ./$(TARGET) input_tiny_ascii.data output_tiny_ascii.test
	diff ./output_tiny_ascii.data ./output_tiny_ascii.test

	time ./$(TARGET) input_tiny_skewed.data output_tiny_skewed.test
	diff ./output_tiny_skewed.data ./output_tiny_skewed.test

	time ./$(TARGET) input_tiny.data output_tiny.test
	diff ./output_tiny.data ./output_tiny.test
