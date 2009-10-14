from math import log

def dimensions(arr):
    if isinstance(arr, list):
        if len(arr) == 0:
            return 1
        return dimensions(arr[0])+1
    return 0

def array_dim_lengths(arr):
    d = dimensions(arr)
    l = []
    if d <= 1:
        return  [1, len(arr)]
    for i in xrange(d):
        if len(arr) == 0:
            l.append(0)
            break
        l.append(len(arr))
        arr = arr[0]
    return l

d = [  [[], [1,0]],
       [[1], [1,1]],
       [[1,2,3], [1,3]],
       [[[1,2,3],
         [1,2,3]], [2,3]],
       [[[[1,2,3],
          [1,2,3]
         ],
         [[1,2,3],
          [1,2,3]
         ]
        ],
        [2,2,3]],
       ]

for data, expected in d:
    r = array_dim_lengths(data)
    assert r == expected, "%s != %s"%(r, expected)
    