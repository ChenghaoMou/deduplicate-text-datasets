#!/usr/bin/env python
import os
from collections import deque

def restore(text_file: str, seg_file: str, ids_file: str):

    indices = deque([])
    with open(seg_file) as f:
        for line in f:
            x, y = line.strip().split(" ", 1)
            if x.isdigit() and y.isdigit():
                indices.append((int(x), int(y)))
    
    # print(min(i[0] for i in indices), max(i[1] for i in indices))
    # file_size = os.path.getsize(text_file)
    # print(f"File Size is :{file_size} bytes")
    # exit(0)

    index2id = {}
    with open(ids_file) as f:
        for idx, id in enumerate(f):
            index2id[idx] = str(id).strip('\n')
    
    with open(text_file, "rb") as f:
        idx = 0
        start = 0
        line = f.readline()
        while line:
            end = start + len(line)
            while indices:
                x, y = indices.popleft()
                while y <= start and indices:
                    x, y = indices.popleft()
                
                if y <= start:
                    break

                if x >= end:
                    indices.appendleft((x, y))
                    break
                
                if start <= x < end <= y:
                    yield idx, index2id[idx], (x - start, end - start)
                    if y > end:
                        indices.appendleft((end, y))
                    break
                elif start <= x < y <= end:
                    yield idx, index2id[idx], (x - start, y - start)
                    continue
                elif x < start < y <= end:
                    yield idx, index2id[idx], (0, y - start)
                    continue
                elif x < start < end <= y:
                    yield idx, index2id[idx], (0, end - start)
                    if y > end:
                        indices.appendleft((end, y))
                    break
            start = end
            idx += 1
            line = f.readline()
    
    assert len(indices) == 0, f"{len(indices)} {idx}"


if __name__ == "__main__":

    import typer

    def main(text_file, sa_file, id_file):
    
        print("id\tx\ty")
        for _, id, (x, y) in restore(
            text_file, 
            sa_file,
            id_file,
        ):
            if y - x >= 50:
                print(f"{id}\t{x}\t{y}")
    
    typer.run(main)