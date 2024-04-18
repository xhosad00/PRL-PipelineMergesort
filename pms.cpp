/***
 * @author Adam Hos <xhosad00>
 * @file implementation of Pipeline merge sort in openMPI
 * @brief reads unsorted input stored in file 'numbers' in the same directory. Numberes are stored as bytes with no spaces and can be generated using the num.sh script
*/
#include <mpi.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <queue>
#include <math.h>


// floor val will be sent if the input is not of length == power of 2. It has to be out of range of input numbers
const int FLOOR_VALUE = 300 ;

/***
 * @brief send one number from top queue to the dst processor 
 * @param sendTop how many numbers were sent from top queue in last batch
 * @param top top queue
 * @param rank rank of this processor
 * @param dst rank of the processor where this processor sends data
*/
void sendNumFromTop(int& sendTop, std::queue<int>& top,int rank, int dst) // TODO delete rank
{
	int num = top.front();
	top.pop();
	++sendTop;
	MPI_Send(&num, 1, MPI_INT, dst, 0, MPI_COMM_WORLD);
}

/***
 * @brief send one number from bottom queue to the dst processor 
 * @param sendBottom how many numbers were sent from bottom queue in last batch
 * @param bottom bottom queue
 * @param rank rank of this processor
 * @param dst rank of the processor where this processor sends data
*/
void sendNumFromBottom(int& sendBottom, std::queue<int>& bottom,int rank, int dst) // TODO delete rank
{
	int num = bottom.front();
	bottom.pop();
	++sendBottom;
	MPI_Send(&num, 1, MPI_INT, dst, 0, MPI_COMM_WORLD);
}

/***
 * @brief send one number from queues to the dst processor based. Number is selected by Pipeline merge sort alg.
 * @param sendTop how many numbers were sent from top queue in last batch
 * @param sendBottom how many numbers were sent from bottom queue in last batch
 * @param startSortingAt when can the processor start sorting 
 * @param top top queue
 * @param bottom bottom queue
 * @param rank rank of this processor
 * @param dst rank of the processor where this processor sends data
 * @return return true if batch was finished
*/
bool sendNum(int& sendTop, int& sendBottom, const int startSortingAt, std::queue<int>& top,std::queue<int>& bottom, int rank,  int dst) // TODO delete rank
{
	if (sendTop == startSortingAt) // allready sent all numbers from top queue in this batch
	{
		sendNumFromBottom(sendBottom, bottom, rank, dst);
	}
	else if (sendBottom == startSortingAt) // allready sent all numbers from bottom queue in this batch
	{
		sendNumFromTop(sendTop, top, rank, dst);
	}
	else if (top.front() <= bottom.front()) // decide witch is lower
	{
		sendNumFromTop(sendTop, top, rank, dst);
	}
	else
	{
		sendNumFromBottom(sendBottom, bottom, rank, dst);
	}

	if (sendTop + sendBottom == startSortingAt * 2) // finished batch, reset counters
	{
		sendTop = 0;
		sendBottom = 0;
		return true;
	}
	return false;
}


/***
 * @brief send all numbers in this batch
 * @param sendTop how many numbers were sent from top queue in last batch
 * @param sendBottom how many numbers were sent from bottom queue in last batch
 * @param startSortingAt when can the processor start sorting 
 * @param top top queue
 * @param bottom bottom queue
 * @param rank rank of this processor
 * @param dst rank of the processor where this processor sends data
*/
void sendAll(int& sendTop, int& sendBottom, const int startSortingAt, std::queue<int>& top,std::queue<int>& bottom, int rank, int dst)
{
	if (sendTop != 0 || sendBottom != 0)
	{
		bool ret;
		do  // send the rest
		{
			ret = sendNum(sendTop, sendBottom, startSortingAt, top, bottom, rank, dst);
		} while (!ret);
	}
}

void flushNum(std::queue<int>& top,std::queue<int>& bottom, int rank,  int dst)
{
	bool sendTop = true;
	if (top.empty()) // allready sent all numbers from top queue in this batch
	{
		sendTop = false;
	}
	else if (bottom.empty()) // allready sent all numbers from bottom queue in this batch
	{
		sendTop = true;
	}
	else if (top.front() <= bottom.front()) // decide witch is lower
	{
		sendTop = true;
	}
	else
	{
		sendTop = false;
	}

	if(sendTop)
	{		
		int num = top.front();
		top.pop();
		MPI_Send(&num, 1, MPI_INT, dst, 0, MPI_COMM_WORLD);
	}
	else
	{
		int num = bottom.front();
		bottom.pop();
		MPI_Send(&num, 1, MPI_INT, dst, 0, MPI_COMM_WORLD);
	}
}

/***
 * @brief the processor recieved FLOOR_VALUE, sort all values and send them (including FLOOR_VALUE). Also process last batch of numbers if needed
 * @param sendTop how many numbers were sent from top queue in last batch
 * @param sendBottom how many numbers were sent from bottom queue in last batch
 * @param startSortingAt when can the processor start sorting 
 * @param top top queue
 * @param bottom bottom queue
 * @param rank rank of this processor
 * @param dst rank of the processor where this processor sends data
*/
void flushQueues(int& sendTop, int& sendBottom, const int startSortingAt, std::queue<int>& top,std::queue<int>& bottom, int rank, int dst) // TODO delete rank
{
	int remainingInBatch = startSortingAt * 2 - (sendTop + sendBottom);
	int diff = remainingInBatch - top.size() - bottom.size();
	if (diff < 0) // send all in remaining batch
		sendAll(sendTop, sendBottom, startSortingAt, top, bottom, rank, dst);
	
	// send the last incomplete batch with FLOOR_VALUE
	int i = 0;
	int remaining = top.size() + bottom.size();
	for (int k = 0; k < remaining; ++k)
	{
		flushNum(top, bottom, rank, dst);
	} 
}

/***
 * @brief check if number is a power of two
 * @param x number
*/
bool isPowerOfTwo(int x) 
{
	if (x == 0)
		return false;
    return (x & (x - 1)) == 0;
}

int main(int argc, char** argv) {
	// MPI init
	MPI_Init (&argc, &argv);
	int world_size, rank;
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	// max file input length for this amount of processors
	const int maxInputLength = pow(2, world_size - 1);
	const int lastRank = world_size - 1;	// rank of last processor

	// printf("Rank %d out of %d processors\n", rank, world_size);
	
	int seenCnt = 0; // how many nubers did the process see
	if (rank == 0) // first processor, sends numbers and prints data
	{
		std::ifstream file("numbers", std::ios::binary); // Open file in binary mode
		if (!file) 
		{
			std::cerr << "Failed to open file." << std::endl; 
		}

		//read data from file
		char numRead;
		std::queue<int> numAll;
		while (file.read(&numRead, sizeof(numRead))) 
		{
			int num = (int)(unsigned char)numRead; // convert char -> usigned char -> int
			numAll.push(num);
			MPI_Send(&num, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);	// send to next processor
		}
		file.close();
		int len = numAll.size();

		
		//print input
		int lastNum = numAll.back();
		while (!numAll.empty())
		{
			int n = numAll.front();
			std::cout << (int)n;
			if (n != lastNum)
				std::cout << " "; 
			numAll.pop();
		}
		std::cout << "\n";
		
		// send floor val if input len != power of two
		if (!isPowerOfTwo(len))
		{
			MPI_Send(&FLOOR_VALUE, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
		}


		// receive data from last processor
		int seenNumbers = 0, recvNum;
		do
		{	
			MPI_Recv(&recvNum, 1, MPI_INT, lastRank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);	
			std::cout << recvNum << "\n";

			++seenNumbers;
		} while (seenNumbers < len);


	}
	else // sorting processors
	{
		// processor with rank != 1
		int seenNumbers = 0, batchSeen = 0;
		const int startSortingAt = pow(2, rank - 1); // can start when first queue is at 2^(i - 1) elements
		bool waitingForNumbers = true;
		std::queue<int> top, bottom;
		int sendTop = 0, sendBottom = 0;
		int dst = rank + 1;	// who is the next processor
		if (rank == lastRank)
			dst = 0;

		int recvNum = 0;
		bool flushed = false;
		do
		{	
			MPI_Recv(&recvNum, 1, MPI_INT, rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE); // receive num from previous
			if (recvNum == FLOOR_VALUE) // Got floor value, push it to bottom and flush queues
			{
				bottom.push(FLOOR_VALUE);
				flushed = true;
				break;
			}
			if (batchSeen < startSortingAt) //add to top queue until seen startSortingAt amount
			{
				top.push(recvNum);
			}
			else	// push to bottom queue 
			{
				waitingForNumbers = false; // set flag for batch processing
				bottom.push(recvNum);
			}
			++batchSeen;

			if (!waitingForNumbers ) // process batch of numbers where size(batch) == 2 * startSortingAt 
			{						 // elements from bottom queue do not have to be received allready, they can come later
				sendNum(sendTop, sendBottom, startSortingAt, top, bottom,rank , dst);
			}
			if (batchSeen == startSortingAt * 2) // processed complete batch, reset
			{
				batchSeen = 0;
			}
			++seenNumbers;
		} while (seenNumbers < maxInputLength); // until seen all numbers 

		if (!flushed)	// didnt flush, input is of size 2^n
		{
			if (top.size() != 0 || bottom.size() != 0 ) // send the rest if some remain
			{
				sendAll(sendTop, sendBottom, startSortingAt, top, bottom, rank, dst);
			}
		}
		else
		{
			// handle sending with FLOOR_VALUE
			flushQueues(sendTop, sendBottom, startSortingAt, top, bottom, rank, dst);
		}
	}

	// end process
	MPI_Finalize();
}