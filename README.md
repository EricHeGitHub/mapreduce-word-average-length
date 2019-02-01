Project description:
Given a text file, compute the average length of words starting with each letter. This means that for every letter, you need to compute: the total length of all words that start with that letter divided by the total number of words that start with that letter.
1.Ignore the letter case, i.e., consider all words as lower case.
2.Ignore terms starting with non-alphabetical characters, i.e., only
consider terms starting with “a” to “z”.
3.The length of the term is obtained by the length() function of String.E.g., the length of “text234sdf” is 10.
4.Use the tokenizer below to split the documents into terms.
StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
5.You do not need to configure the numbers of mappers and reducers. Default values will be used.

Input files:
Text documents

Output format:

MapReduce job should generate a list of key-value pairs, and ranked in alphabetical order, like (this example is only used to show the format):

a 3.5613186813186815,

b 4.3849323131253675,

...... ......

z 7.909090909090909

The average length is of double precision (use DoubleWritable).