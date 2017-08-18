package com.dataminer.util;

import static scala.collection.JavaConversions.collectionAsScalaIterable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import scala.collection.immutable.$colon$colon$;
import scala.collection.immutable.List$;

public class CollectionUtil {

    public static <T> List<T> flattenOptionalList(List<Optional<T>> optionalList) {
        List<T> flattenedList = Lists.newArrayList();

        for (Optional<T> opt : optionalList) {
            if (opt.isPresent()) {
                flattenedList.add(opt.get());
            }
        }

        return flattenedList;
    }

    /**
     * @param collection 集合类
     * @param func 对于每一个元素施加的操作
     * @param sep 分割符  , : + ...
     * @param <T> 传入集合的元素类型
     * @return 集合按照某种规则拼接的字符串
     *
     * ArrayList(1, 2, 3), e -> e + 1, ; ==> 2;3;4
     */
    public static <T> String mkString(Collection<T> collection, Function<T, String> func, String sep) {
        int counter = 0;
        StringBuilder sb = new StringBuilder();

        for (T e: collection) {
            if (counter != 0) {
                sb.append(sep).append(func.apply(e));
            } else {
                sb.append(func.apply(e));//Fix bug the first element not call func
            }
            counter ++;
        }

        return sb.toString();
    }

    public static scala.collection.immutable.List scalaNil = List$.MODULE$.empty();

    /*
        scala.collection.Seq<String> seq = CollectionUtil.asScalaSeq("a","b","c");

        // if you wanna pass List to varargs, a little trick should be used
        List<String> arrs = Arrays.asList("a","b","c");
        scala.collection.Seq<String> seq = CollectionUtil.asScalaSeq(arrs.toArray(new String[arrs.size()]));

         the seq is actually stream by default, so result is `Stream(a, ?)`, to get all
         seq.toList()
         scala.collection.immutable.List("a", "b", "c")
    */
    public static <T> scala.collection.Seq<T> asScalaSeq(T... elements) {
        // def toSeq: Seq[A] = toStream
        return collectionAsScalaIterable(Arrays.asList(elements)).toSeq();
    }

    public static <T> scala.collection.immutable.List<T> asScalaList(T... elements) {
        scala.collection.immutable.List<T> resultList = null;
        boolean head = true;

        for (T e: elements) {
            if (head) {
                resultList = $colon$colon$.MODULE$.apply(e, scalaNil);
                head = false;
            } else {
                resultList = $colon$colon$.MODULE$.apply(e, resultList);
            }
        }

        return (resultList == null) ? scalaNil : resultList.reverse();
    }
}