package com.wds.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import name.fraser.neil.plaintext.diff_match_patch;
import name.fraser.neil.plaintext.diff_match_patch.Diff;
import name.fraser.neil.plaintext.diff_match_patch.Operation;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by denglish on 6/12/17.
 */
public class TextUtil
{
   private static final String INDENT = "  ";

   private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

   public static String ppJson(final String json)
   {
      final JsonParser jp = new JsonParser();
      final JsonElement je = jp.parse(json);
      return gson.toJson(je);
   }

   public static String indent(final String text, final String indent)
   {
      return indent + StringUtils.join(text.split("\n"), "\n" + indent);
   }

   public static String toText(final String title,
                               final Collection<?> collection)
   {
      return title + "\n" + StringUtils.join(collection, "\n" + INDENT);
   }

   private static class SplitLine {
      public String lineFragment;
      public final LinkedList<String> lines = new LinkedList<>();
      public SplitLine(final Diff diff) {
         String tag = "";
         switch(diff.operation) {
            case INSERT: tag = "Ins"; break;
            case DELETE: tag = "Del"; break;
         }
         final boolean lastCharIsNewline = diff.text.charAt(diff.text.length()-1) == '\n';
         final String totalText = StringUtils.isEmpty(tag) ?
                                  diff.text :
                                  ("[" + tag + "]" + diff.text + "[E" + tag + "]");
         if(totalText.equals("\n")) {
            lines.add("");
            this.lineFragment = "";
         } else {
            final String[] parts = totalText.split("\n");
            for(int i = 0; i < parts.length; ++i) {
               lines.add(parts[i]);
            }
            this.lineFragment = lastCharIsNewline ? "" : lines.removeLast();
         }
      }
   }

   private static class ParsedLine {
      public final boolean hadChange;
      public final String line;
      public final int lineNumber;
      public ParsedLine(final boolean hadChange,
                        final String line,
                        final AtomicInteger ai) {
         this.hadChange = hadChange;
         this.line = line;
         this.lineNumber = ai.incrementAndGet();
      }
   }

   private static class LineLoader {
      private final List<ParsedLine> lines = new ArrayList<>();
      private final AtomicInteger ai = new AtomicInteger(0);
      public void addSection(final SplitLine sl,
                             final SplitLine lastSl,
                             final Operation op) {
         String lineFragment = lastSl == null ? "" : lastSl.lineFragment;

         if((sl.lines.size() == 0) && (!StringUtils.isEmpty(sl.lineFragment))) {
            sl.lineFragment = lastSl.lineFragment + sl.lineFragment;
         } else {
            for (final String line : sl.lines) {
               final StringBuilder sb = new StringBuilder();
               sb.append(lineFragment);
               sb.append(line);
               sb.append("\n");
               final boolean lineHasChanged = (lastSl == null ?
                  !op.equals(Operation.EQUAL) :
                  (StringUtils.isEmpty(lineFragment) ?
                     !op.equals(Operation.EQUAL) :
                     true));
               lines.add(new ParsedLine(lineHasChanged, sb.toString(), ai));
               lineFragment = "";
            }
         }
      }

      @Override
      public String toString()
      {
         return toString(false);
      }
      public String toString(final boolean onlyChanges) {
         final StringBuilder sb = new StringBuilder();
         for (final ParsedLine line : lines) {
            if(onlyChanges) {
               if(line.hadChange) {
                  sb.append(String.format("%04d: ", line.lineNumber))
                    .append(line.line);
               }
            } else {
               sb.append(String.format("%04d: ", line.lineNumber))
                 .append(line.hadChange ? "*" : " ")
                 .append(line.line);
            }
         }
         return sb.toString();
      }
   }

   public static String diff(final String left,
                             final String right)
   {
      return diff(left, right, true);
   }

   public static String diff(final String left,
                             final String right,
                             final boolean onlyChanges)
   {
      diff_match_patch difference = new diff_match_patch();
      LinkedList<Diff> deltas = difference.diff_main(left, right);

      // Reconstruct texts from the deltas
      //  text1 = all deletion (-1) and equality (0).
      //  text2 = all insertion (1) and equality (0).

      final LineLoader lineLoader = new LineLoader();
      SplitLine lastSl = null;
      for (Diff d : deltas)
      {
         final SplitLine sl = new SplitLine(d);
         lineLoader.addSection(sl, lastSl, d.operation);
         lastSl = sl;
      }
      return lineLoader.toString(onlyChanges);
   }

   public static String diffEff(final String left,
                                final String right)
   {
      diff_match_patch difference = new diff_match_patch();
      LinkedList<Diff> deltas = difference.diff_main(left, right);

      // Reconstruct texts from the deltas
      //  text1 = all deletion (-1) and equality (0).
      //  text2 = all insertion (1) and equality (0).

      final StringBuilder sb = new StringBuilder();
      //int lineNumber = 1;
      boolean firstOutputLine = true;
      for (Diff d : deltas)
      {
         //lineNumber += StringUtils.countMatches(d.text, "\n");
         switch(d.operation) {
            case EQUAL: {
               final boolean lastCharIsNewline = d.text.length() == 0 ?
                  false :
                  d.text.charAt(d.text.length() - 1) == '\n';
               final int pos = d.text.lastIndexOf('\n');
               final String lastLine = (pos == -1 ?
                                       d.text :
                                       (lastCharIsNewline ?
                                          d.text :
                                          d.text.substring(pos)));
               if(firstOutputLine) {
                  if(    (lastLine.length() > 0)
                      && (lastLine.charAt(0) == '\n')) {
                     sb.append(lastLine.substring(1));
                  } else {
                     sb.append(lastLine);
                  }
                  firstOutputLine = false;
               } else {
                  sb.append(lastLine);
               }
            }
            break;
            case INSERT: {
               sb.append("[Ins]").append(d.text).append("[EIns]");
            }
            break;
            case DELETE: {
               sb.append("[Del]").append(d.text).append("[EDel]");
            }
            break;
         }
      }

      return sb.toString();
   }

   public static String rawDiff(final String left,
                                final String right)
   {
      diff_match_patch difference = new diff_match_patch();
      LinkedList<Diff> deltas = difference.diff_main(left, right);

      // Reconstruct texts from the deltas
      //  text1 = all deletion (-1) and equality (0).
      //  text2 = all insertion (1) and equality (0).
      StringBuilder sb = new StringBuilder();
      for (Diff d : deltas)
      {
         sb.append(d.toString()).append("\n");
      }

      return sb.toString();
   }

   public static <T> String printCollection(final String title,
                                            final Collection<T> collection)
   {
      return printCollection("", title, collection);
   }

   public static <T> String printCollection(final String indent,
                                            final String title,
                                            final Collection<T> collection)
   {
      final StringBuilder sb = new StringBuilder();
      sb.append(indent).append(title).append("[").append(collection.size()).append("]:\n");
      int count = 0;
      for(final T t : collection) {
         sb.append(indent)
           .append("  ")
           .append(title)
           .append("[")
           .append(count++)
           .append("]: ")
           .append(t)
           .append("\n");
      }
      return sb.toString();
   }

   public static <K, V> String printMap(final String title,
                                        final Map<K, V> map)
   {
      return printMap("", title, map);
   }

   public static <K, V> String printMap(final String indent,
                                        final String title,
                                        final Map<K, V> map)
   {
      final StringBuilder sb = new StringBuilder();
      sb.append(indent).append(title).append("[").append(map.size()).append("]:\n");
      int count = 0;
      for(final Entry<K, V> e : map.entrySet()) {
         sb.append(indent)
           .append("  ")
           .append(title)
           .append("[")
           .append(e.getKey())
           .append("]: ")
           .append(e.getValue())
           .append("\n");
      }
      return sb.toString();
   }
}
