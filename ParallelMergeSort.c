#include "mpi.h"
#include "stdlib.h"
#include "time.h"
#include "stdio.h"

int* Merge(int* sortedArray1, int* sortedArray2, int sortedArray1Length, int sortedArray2Length){

   int* sortedArray = (int*)malloc((sortedArray1Length + sortedArray2Length) * sizeof(int));
   int i = 0, sortedArray1Ind = 0, sortedArray2Ind = 0;

   for (i = 0; i < sortedArray1Length + sortedArray2Length; ++i) {
      if ((sortedArray1Ind < sortedArray1Length) && (sortedArray2Ind < sortedArray2Length)){
         if (sortedArray1[sortedArray1Ind] < sortedArray2[sortedArray2Ind]){
            sortedArray[i] = sortedArray1[sortedArray1Ind];
            sortedArray1Ind++;
         } else {
              sortedArray[i] = sortedArray2[sortedArray2Ind];
              sortedArray2Ind++;
           }
      } else if (sortedArray1Ind < sortedArray1Length) {
           sortedArray[i] = sortedArray1[sortedArray1Ind];
           sortedArray1Ind++;
        } else {
             sortedArray[i] = sortedArray2[sortedArray2Ind];
             sortedArray2Ind++;
          }
   }
   return sortedArray;
}

int* MergeSort(int* arrayToSort, int* bufArray, int leftBound, int rightBound) {

   int i = 0;

   if (leftBound == rightBound) {
      bufArray[leftBound] = arrayToSort[leftBound];
      return bufArray;
   }

   int midInd = (leftBound + rightBound) / 2;

   int *leftBufArray = MergeSort(arrayToSort, bufArray, leftBound, midInd);
   int *rightBufArray = MergeSort(arrayToSort, bufArray, midInd + 1, rightBound);

   int *sortingArray = leftBufArray == arrayToSort ? bufArray : arrayToSort;

   int leftCurrent = leftBound, rightCurrent = midInd + 1;

   for (i = leftBound; i <= rightBound; ++i) {
      if ((leftCurrent <= midInd) && (rightCurrent <= rightBound)){
         if (leftBufArray[leftCurrent] < rightBufArray[rightCurrent]){
            sortingArray[i] = leftBufArray[leftCurrent];
            leftCurrent++;
         } else {
              sortingArray[i] = rightBufArray[rightCurrent];
              rightCurrent++;
           }
      } else if (leftCurrent <= midInd) {
           sortingArray[i] = leftBufArray[leftCurrent];
           leftCurrent++;
        } else {
             sortingArray[i] = rightBufArray[rightCurrent];
             rightCurrent++;
          }
   }
   return sortingArray;
}

int main(int argc, char *argv[]){

   int size = 100000000;
   srand(time(NULL));

   int procRank = 0, procNum = 0, nameLen = 0, i = 0;
   char procName[MPI_MAX_PROCESSOR_NAME];
   double startTime = 0, endTime = 0;

   int* arrayToSort;
   int* bufArray;
   int* sortedArray;

   MPI_Init(&argc, &argv);
   MPI_Comm_size(MPI_COMM_WORLD, &procNum);
   MPI_Comm_rank(MPI_COMM_WORLD, &procRank);
   MPI_Get_processor_name(procName, &nameLen);
   MPI_Status Status;

   fprintf(stderr, "Process %d on %s\n", procRank, procName);

   if(procRank == 0){

      arrayToSort = (int*)malloc(size * sizeof(int));

      for(i = 0; i < size; ++i){
         arrayToSort[i] = rand();
      }

      /*for(i = 0; i < size; ++i){
         printf("%d ", arrayToSort[i]);
         printf("\n\n");
      }*/
   }

   startTime = MPI_Wtime();

   if((size < procNum) && (procRank == 0)){

      int* bufArray = (int*)malloc(size * sizeof(int));

      sortedArray = MergeSort(arrayToSort, bufArray, 0, size - 1);

      if(sortedArray == arrayToSort){
         free(bufArray);
      } else {
           free(arrayToSort);
           arrayToSort = bufArray;
      }
      arrayToSort = sortedArray;
   } else if (size >= procNum) {
        int* sendCount = (int*)malloc(procNum * sizeof(int));
        int* sendPoint = (int*)malloc(procNum * sizeof(int));

        for(i = 0; i < procNum; ++i){
           sendCount[i] = size / procNum;
           sendPoint[i] = i * sendCount[i];
        }
        sendCount[procNum - 1] += size % procNum;

        int* subArray = (int*)malloc(sendCount[procRank]* sizeof(int));

        MPI_Scatterv(arrayToSort, sendCount, sendPoint, MPI_INT, subArray, sendCount[procRank], MPI_INT, 0, MPI_COMM_WORLD);

        if(procRank == 0){
           free(arrayToSort);
        }

        bufArray = (int*)malloc(sendCount[procRank] * sizeof(int));

        sortedArray = MergeSort(subArray, bufArray, 0, sendCount[procRank] - 1);

        if(sortedArray == subArray){
           free(bufArray);
        } else {
             free(subArray);
             sortedArray = bufArray;
        }

        bufArray = sortedArray;
        int procRankInd = 0, mergeIndProc = procNum, sendLength = sendCount[procRank], recvLength = 0;

        free(sendCount);
        free(sendPoint);

        while(mergeIndProc > 1){
           for(i = 0; i < mergeIndProc / 2; ++i){
              if(procRank == mergeIndProc - i - 1){
                 MPI_Send(&sendLength, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                 MPI_Send(bufArray, sendLength, MPI_INT, i, 0, MPI_COMM_WORLD);
              } else if(procRank == i){
                   MPI_Recv(&recvLength, 1, MPI_INT, mergeIndProc - i - 1, 0, MPI_COMM_WORLD, &Status);
                   subArray = (int*)malloc(recvLength * sizeof(int));
                   MPI_Recv(subArray, recvLength, MPI_INT, mergeIndProc - i - 1, 0, MPI_COMM_WORLD, &Status);

                   arrayToSort = Merge(bufArray, subArray, sendLength, recvLength);
                   sendLength += recvLength;
                   free(bufArray);
                   free(subArray);
                   bufArray = arrayToSort;
              }
           }
              mergeIndProc -= mergeIndProc / 2;
              MPI_Barrier(MPI_COMM_WORLD);
        }

        free(bufArray);
   }

   endTime = MPI_Wtime();

   if(procRank == 0){
      /*for(i = 0; i < size; ++i){
         printf("%d ", arrayToSort[i]);
      }
      printf("\n");*/

      printf("\nTime = %f\n", (endTime - startTime));
      free(arrayToSort);
   }

   MPI_Finalize();
   return 0;
}
