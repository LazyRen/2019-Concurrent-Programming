# Compiler and Compile options.
CC = g++
CFLAGS = -Wall -g -O2

# Macros specifying path for compile.
SRCS := $(wildcard src/*.cpp)
INCS := $(wildcard include/*.h)
INC = ./include/
LIB = ./lib/

# Pre-Processor.
CFLAGS += -std=c++14 -I$(INC) -L$(LIB) -lpthread

TARGET = run

all: $(TARGET)

$(TARGET): $(SRCS) $(INCS)
	$(CC) -o $(TARGET) $(SRCS) $(CFLAGS)

# Delete binary & object files
clean:
	$(RM) $(TARGET) $(OBJS)
	$(RM) ./output_tiny_ascii.test ./output_tiny_skewed.test ./output_tiny.test

test:
	make
	time ./$(TARGET) input_tiny_ascii.data output_tiny_ascii.test
	diff ./output_tiny_ascii.data ./output_tiny_ascii.test

	time ./$(TARGET) input_tiny_skewed.data output_tiny_skewed.test
	diff ./output_tiny_skewed.data ./output_tiny_skewed.test

	time ./$(TARGET) input_tiny.data output_tiny.test
	diff ./output_tiny.data ./output_tiny.test
