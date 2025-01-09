def findMedian(arr):
    # Write your code here
    arr = sorted(arr)
    n = len(arr) // 2
    print(arr[n])

findMedian([0,1,2,4,6,5,3])