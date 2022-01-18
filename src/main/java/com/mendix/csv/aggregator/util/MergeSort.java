package com.mendix.csv.aggregator.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class MergeSort
{

    static public List<String> sort(List<String> its)
    {
        int len = its.size();
        if ( len <= 1 )
        {
            return its;
        }

        List<String> left = sort( its.subList( 0, len/2) );
        List<String> right = sort( its.subList(len/2, len) );
        return merge(left, right);
    }

    static public List<String> merge(List<String> listA, List<String> listB)
    {
        int lenListA = listA.size();
        int lenListB = listB.size();

        if (lenListA <= 0) return listA;
        if (lenListB <= 0) return listB;

        if (listA.get(0).compareTo(listB.get(0)) <= 0)
        {
            List<String> lLeft = merge(subList(listA, 1, lenListA), listB);
            lLeft.add(0, listA.get(0));

            return lLeft;
        }
        else
        {
            List<String> lRight = merge(listA, subList(listB, 1, lenListB));
            lRight.add(0, listB.get(0) );


            return lRight;
        }
    }

    static public final List<String> subList(List<String> items, int start, int endExclusive)
    {
        return new ArrayList<String>(items.subList(start, endExclusive));
    }

    public static void main(String[] args) {

        List<String> arr = new ArrayList<String>(Arrays.asList("z", "a", "b", "t", "k", "m"));

        List<String> r = MergeSort.sort(arr);

        System.out.println(r.size());
        System.out.println(arr.size());

        Stream.of(r).forEach(System.out::println);
    }
}