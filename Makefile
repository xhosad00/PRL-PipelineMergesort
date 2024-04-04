GHC = mpic++
# GHC_FLAGS = 

# Source file
SOURCE = pms.cpp

# Output executable
EXECUTABLE = pms

# Default target
all: $(EXECUTABLE)

# Rule to build the executable
$(EXECUTABLE): $(SOURCE)
	$(GHC) $(SOURCE) -o pms

clean:
	rm -f pms
