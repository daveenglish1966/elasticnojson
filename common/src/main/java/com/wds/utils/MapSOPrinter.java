package com.wds.utils;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Created by denglish on 6/1/17.
 */
public class MapSOPrinter
{
   private static String INDENT = "  ";

   public static String printDiff(final Map<String, Object> mapl,
                                  final Map<String, Object> mapr)
   {
      return TextUtil.diff(printMap(mapl), printMap(mapr), true);
   }

   public static String printMap(final Map<String, Object> map)
   {
      return printMap("", map);
   }

   public static String printMap(final String indent,
                                 final Map<String, Object> map)
   {
      final StringBuilder sb = new StringBuilder("{\n");
      printMap(sb, indent, map);
      sb.append("}\n");
      return TextUtil.ppJson(sb.toString());
   }

   private static void printList(final StringBuilder sb,
                                 final String indent,
                                 final List<Object> list)
   {
      String delim = "  ";
      for(final Object object : list) {
         sb.append(indent)
           .append(delim);
         if(!printType(sb, indent, object)) {
            sb.append("\n");
         }
         delim = ", ";
      }
   }

   private static void printMap(final StringBuilder sb,
                                final String indent,
                                final Map<String, Object> map)
   {
      String delim = "  ";
      final Map<String, Object> sortedMap = new TreeMap<>();
      sortedMap.putAll(map);
      for (final Entry<String, Object> entry : sortedMap.entrySet()) {
         sb.append(indent)
           .append(delim)
           .append("\"" + entry.getKey() + "\": ");
         printType(sb, indent, entry.getValue());
         sb.append("\n");
         delim = ", ";
      }
   }

   private static boolean printType(StringBuilder sb, String indent, Object value)
   {
      switch (value.getClass().getName()) {
         case "java.lang.Boolean" :
            final Boolean b = (Boolean)value;
            sb.append(b.toString());
            return false;
         case "java.lang.Double" :
            final Double d = (Double)value;
            sb.append(d.toString());
            return false;
         case "java.lang.Integer" :
            final Integer integer = (Integer)value;
            sb.append(integer.toString());
            return false;
         case "java.lang.Long" :
            final Long l = (Long)value;
            sb.append(l.toString());
            return false;
         case "java.lang.String" :
            sb.append("\"" + ((String)value).replace("\"", "\\\"") + "\"");
            return false;
         case "java.util.ArrayList" :
            sb.append("[").append("\n");
            final List<Object> list = (List<Object>)value;
            printList(sb, indent + INDENT, list);
            sb.append(indent).append("]");
            return true;
         case "java.util.LinkedHashMap" :
         case "java.util.HashMap" :
            sb.append("{").append("\n");
            printMap(sb, indent + INDENT, (Map<String, Object>)value);
            sb.append(indent);
            sb.append("}");
            return true;
         default:
            sb.append(value.getClass().getName());
            return false;
      }
   }
}
