# Deduplicating Training Data Makes Language Models Better

This repository contains code to deduplicate language model datasets as descrbed in the paper ["Deduplicating Training Data Makes Language Models Better"](https://arxiv.org/abs/2107.06499) by Katherine Lee, Daphne Ippolito, Andrew Nystrom, Chiyuan Zhang, Douglas Eck, Chris Callison-Burch and Nicholas Carlini.
We release the ExactSubstr deduplication implementation (written in Rust) along with the scripts we used in the paper to perform ExactSubstr deduplication and inspect the results (written in Python).
We also release the document clusters resulting from running NearDup deduplication on C4, RealNews, LM1B, and Wiki-4B-en.

This is not an officially supported Google product.

## Why deduplicate?
When datasets are created by scraping raw text from the Internet, this will often result in the same sequences being repeated multiple times (e.g., we find a single 50 word sequence that is repeated in the C4 dataset 60,000 times).
Training models on deduplicated datasets is faster (because they see fewer total examples) and experimentally results in models with similar or better perplexity to models trained on data that hasn't been deduplicated. Moreover, language models are less likely to exhibit memorization when their training data has been well-deduplicated.

## Citing this work

If you use this repository or our deduplicated datasets you can cite

```
@article{lee2021deduplicating,
      title={Deduplicating Training Data Makes Language Models Better}, 
      author={Katherine Lee and Daphne Ippolito and Andrew Nystrom and Chiyuan Zhang and Douglas Eck and Chris Callison-Burch and Nicholas Carlini},
      journal={arXiv preprint arXiv:2107.06499},
      year={2021},
}
```

# Exact Deduplication Code

We provide an implementation of the exact deduplication technique used in the paper. This is very much research code. It is (a very slightly cleaned up) version of exactly what we do in the paper. It assumes that you want to deduplicate something the size of C4 (~300GB) running on a machine with 96 cores and >600GB of RAM. If you only want to use this for reasonably-sized datasets, you should change the number of parallel threads from 96 to something smaller. If your machine is big enough, there should be no upper bound on the size of the dataset it can handle (well, 2^64-1 bytes is the limit, but I think we can all agree that's essentially unlimited).


We build a suffix array (based on [Andrew Gallant's suffix array implementation](https://github.com/BurntSushi/suffix/)) in [src/table.rs](src/table.rs). It has some minor changes from the original version that make it so we can't just import this library as a crate. First, we need 64-bit integers. The original implementation says that u32 works for "reasonably sized documents (~4GB)" but we're working with unreasonably sized documents. So we need u64. Second, we don't want UTF8 strings. Everything is a [u8] byte array, because we might be working over token sequences which aren't valid UTF8.
The main complication in the rest of [src/main.rs](src/main.rs) is the fact that we want things to run in parallel, and we probably can't fit the entire suffix array into memory. And so all of our algorithms are designed around these constraints.

If you just want to run the rust deduplicator, then you will only need to install Rust:

```curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh```

If you additionally want to generate datasets to run the rust script on (and you probably do) then you will need python dependencies:

```pip3 install numpy scipy tensorflow tensorflow_datasets transformers sentencepiece```

## Basic Usage

If you just want to reproduce the result of this paper, or deduplicate any language model that's already in the [Tensorflow Datasets (TFDS)](https://www.tensorflow.org/datasets) format, then you can just run the following commands:

```cargo build```

to compile the rust code, and then run

```python3 scripts/load_dataset.py --data_dir $LOAD_DIR --save_dir $SAVE_DIR --name $DATASET --split $SPLIT [--tokenize]```

For example, to get the LM1B training set you could run `python3 scripts/load_dataset.py --data_dir ~/tensorflow_datasets --save_dir data --name lm1b --split test`. This should will take just a few seconds to run on the test set or about an hour if running with the `train` set instead.

If the dataset is really big, you might want to add the `--tokenize` flag. This will shrink the dataset by roughly a factor of two by tokenizing it with the GPT-2 tokenizer.

And then to construct the suffix array run

```python3 scripts/make_suffix_array.py [path/to/dataset]```

For example, if you run `python3 scripts/make_suffix_array.py data/lm1b.test`, this will create a file `data/lm1b.test.table.bin` containing the suffix array. Again, this should be fast, about two hours on the LM1B train set when run single-thread and a few minutes on 96 cores.

(If you get an error that you have too many open files, that's because this script opens lots of files. You should run `ulimit -Sn 1000000` to "fix" the error.)

### Querying a suffix array to find duplicated examples

Start by loading and building a suffix array for a dataset as described above

Once you have the suffix array, you now query the dataset to find all occurances of a particular string. To do this, run

```python3 scripts/count_occurances.py --suffix [path/to/suffix_array] [--query query_string] [--query_file /path/to/query]```

On the LM1B test set, running `python3 scripts/count_occurances.py --suffix data/lm1b.test --query " on Tuesday" should return 1288. If you tokenized the dataset, then you should pass `--tokenize` to `count_occurences.py` as well, to get the same result (plus or minus tokenization differences).


If you want to confirm this the outputted number is correct (assuming you haven't tokenized), you can run `cat /tmp/lm1b.test | grep -ao " on Tuesday"` and get the same result.

## Advanced Usage

The above scripts work by calling into the core Rust suffix array deduplicator. If you want to do each step yourself, the following options are available:

### Single threaded suffix array construction

To build a suffix array for any particular file, you can run

```cargo run save [file_path]```

This will create a file called `[file_path].table.bin` which contains the suffix array for the file provided. This algorithm is linear time, but (a) only runs on a single core, and (b) has memory requirement `O(big * len(file))` which is prohibitive for large files.

### Parallel suffix array construction

To build a suffix array for an extremely large file (e.g., ~about as much RAM as available) it is better to run the script

```python scripts/make_suffix_array.py [file_path]```

This script will build the suffix array in parallel by splitting the single file into chunks, generating suffix arrays for each chunk, and then merging the suffix arrays together to form the full suffix array. Note that in general this algorithm is quadratic, but when the maximum substring length is short relative to the total file length (as it is, when generating suffix arrays for N independent training examples) it will never reach this worst case behavior.


The two steps are described below.

#### Building a piece of a suffix array from a piece of a file

The first generats a suffix array from a piece of a file. This is implemented by running

```cargo run save_part [file_path] [byte_start] [byte_end]```

And builds a suffix array for the byte sequence between [byte_start] and [byte_end] for the given file. Multiple of these can be run in parallel to build a suffix array for a file quickly.

#### Merging suffix array pieces to create a single suffix array

Given the several independent suffix arrays, merging them is now just a matter of calling

```cargo run merge_parallel [path_to_partial_suffix_trees,...] [tmp_output_directory]```

to generate a collection of ordered suffix arrays pieces in the output directory. The final step just requires merging these together

```cat [tmp_output_directory]/* > [file_path].table.bin```

### Finding Duplicates

Given a suffix array file, as generated in the prevous section, it can now be queried for interesting statistics.
The simplest operation, counting occurrences of particular substrings, takes O(log(N)) time and O(query_length) memory requirements, (as shown above with `scripts/count_occurances.py`). To do this you can run:

```cargo run count_occurances /path/to/dataset /path/to/query_file```

(Indeed, the python script is just a wrapper that makes calling this nicer, with the option for tokenization.)
This is useful mainly as a commandline interface to interact with the dataset to find interesting properties. To run more sophisticated analysis, use the tools described below:

#### Finding duplicates between two documents

Given a document A and another document B, we can find all duplicates betwen the two by (1) constructing suffix arrays for both, and then (2) linearly walking the suffix arrays in order to find all duplicates of a given length.

Once the suffix array for the dataset has been constructed, this algorithm therefore requires time O(len(dataset) + len(query)) and space O(len(dataset)). It is better to run this algorithm when the number of queries into the dataset is greater than O(len(dataset)/log(len(query))). However note that the prior code requires *disk seeks* and and this implementation is a linear scan through the suffix array table, so in practice there is at least a factor-of-10 speedup here. As a rough order of magnitude, for a dataset with ~100GB, it is faster to run `similar_parallel` when querying with more than a few megabytes of text. Otherwise it is probably faster to run `count_occurances`.

Notice that this command also requires that the entire dataset fits in memory. For many datasets this is not a problem, but the C4 dataset is 350 GB and the Pile dataset is 750 GB (both even after tokenization). The machine must therefore have a lot of RAM for this to work.

```cargo run similar_parallel [dataset1] [dataset2]```

This creates lots of containing the position of all examples in dataset2 that are also in dataset1. (The code could also do the inverse at the same time, if you want to modify it slightly.) However it spits this out in some not-very-useful form: a list of tokens x_i so that dataset2[x_i:x_i+100] is also in dataset1. But this probably has overlaps.

The second step is then to run 

```cargo run collect_similar [dataset2]```. This converts the result to instead compute ranges so that instead we have dataset2[xi:yi] match.

#### Finding duplicates within one document

To find duplicates that are contained within one document (for example, to actually deduplicate a dataset as we do in the paper) run the command

```cargo run selfsimilar_parallel [dataset]```

This will find all repeated substrings contained in the dataset above a given length threshold. Again run collect_similar to find the indexs of repeated examples.

# Approx Deduplication Results

The following CSVs contain three columns: the document ID, a boolean indicating whether or not this document was deleted during deduplication, and a cluster ID.
Documents with the same cluster ID were identified as near-duplicates. For C4 and RealNews, the document ID is the url associated with the document. For Wiki-40B, it is the `wikidata_id`. LM1B coming soon.

**Name**|**Link**|**Size**
:-----:|:-----:|:-----:
C4|[link](https://storage.googleapis.com/gresearch/data_deduplication/c4.tar.gz)|13GB
RealNews|[link](https://storage.googleapis.com/gresearch/data_deduplication/realnews.tar.gz)|1.4GB
Wiki-40B|[link](https://storage.googleapis.com/gresearch/data_deduplication/wiki40b.tar.gz)|26MB
