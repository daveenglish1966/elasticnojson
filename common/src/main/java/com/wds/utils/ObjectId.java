package com.wds.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>A globally unique identifier for objects.</p>
 *
 * <p>Consists of 12 bytes, divided as follows:</p> 
 * <table border="1">
 *     <caption>ObjectID layout</caption>
 *     <tr>
 *         <td>0</td><td>1</td><td>2</td><td>3</td><td>4</td><td>5</td><td>6</td><td>7</td><td>8</td><td>9</td><td>10</td><td>11</td> 
 *     </tr> 
 *     <tr>
 *         <td colspan="4">time</td><td colspan="3">machine</td> <td colspan="2">pid</td><td colspan="3">inc</td> 
 *     </tr> 
 * </table>
 *
 * <p>Instances of this class are immutable.</p>
 */
public class ObjectId implements java.io.Serializable {

   private static final long serialVersionUID = -4415279469780082174L;

   private final static Logger LOG = LoggerFactory.getLogger(ObjectId.class);

   private final int time;
   private final int machine;
   private final int inc;

   private final boolean isNew;

   private static AtomicInteger _nextInc = new AtomicInteger((new java.util.Random()).nextInt() );

   private static final int GENMACHINE;

   //==========================================================================
   // Public static interface
   //==========================================================================
   public static String newId()
   {
      return new ObjectId().toHexString();
   }

   /**
    * Will return date if valid objectId, else thrown an IllegalArgumentException.
    *
    * @param objectIdStr  The string representation of the ObjectId.
    * @return
    */
   public static Date getDate(final String objectIdStr) {
      final ObjectId objectId =  new ObjectId(objectIdStr);
      return new Date(((long)objectId.time) * 1000);
   }

   /**
    * Gets a new object id.
    *
    * @return the new id
    */
   private static ObjectId get(){
      return new ObjectId();
   }

   /**
    * Makes sure a string representation of an oabject is valid.
    *
    * @param
    * @return
    */
   public static boolean isValid(final String objectId){
      if ( objectId == null )
         return false;

      final int len = objectId.length();
      if ( len != 24 )
         return false;

      for ( int i=0; i<len; i++ ){
         char c = objectId.charAt( i );
         if ( c >= '0' && c <= '9' )
            continue;
         if ( c >= 'a' && c <= 'f' )
            continue;
         if ( c >= 'A' && c <= 'F' )
            continue;

         return false;
      }

      return true;
   }

   //==========================================================================
   // Ctors (Note private for both).  This object should used static interface.
   //==========================================================================
   private ObjectId(final String objectId)
   {
      if (!isValid(objectId))
         throw new IllegalArgumentException("invalid ObjectId [" + objectId + "]" );
      byte b[] = new byte[12];
      for ( int i=0; i<b.length; i++ ){
         b[i] = (byte) Integer.parseInt(objectId.substring(i*2 , i*2 + 2) , 16 );
      }
      ByteBuffer bb = ByteBuffer.wrap(b );
      time = bb.getInt();
      machine = bb.getInt();
      inc = bb.getInt();
      isNew = false;
   }

   /**
    * Create a new object id.
    */
   private ObjectId()
   {
      time = (int) (System.currentTimeMillis() / 1000);
      machine = GENMACHINE;
      inc = _nextInc.getAndIncrement();
      isNew = true;
   }

   //==========================================================================
   // Private implementation
   //==========================================================================
   /**
    * Converts this instance into a 24-byte hexadecimal string representation.
    *
    * @return a string representation of the ObjectId in hexadecimal format
    */
   private String toHexString() {
      final StringBuilder buf = new StringBuilder(24);

      for (final byte b : toByteArray()) {
         buf.append(String.format("%02x", b & 0xff));
      }

      return buf.toString();
   }

   /**
    * Convert to a byte array.  Note that the numbers are stored in big-endian order.
    *
    * @return the byte array
    */
   private byte[] toByteArray(){
      byte b[] = new byte[12];
      ByteBuffer bb = ByteBuffer.wrap(b );
      // by default BB is big endian like we need
      bb.putInt(time);
      bb.putInt(machine);
      bb.putInt(inc);
      return b;
   }

   static {

      try {
         // build a 2-byte machine piece based on NICs info
         int machinePiece;
         {
            try {
               StringBuilder sb = new StringBuilder();
               Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
               while ( e.hasMoreElements() ){
                  NetworkInterface ni = e.nextElement();
                  sb.append( ni.toString() );
               }
               machinePiece = sb.toString().hashCode() << 16;
            } catch (Throwable e) {
               // exception sometimes happens with IBM JVM, use random
               LOG.warn(e.getMessage(), e);
               machinePiece = (new Random().nextInt()) << 16;
            }
         }

         // add a 2 byte process piece. It must represent not only the JVM but the class loader.
         // Since static var belong to class loader there could be collisions otherwise
         final int processPiece;
         {
            int processId = new java.util.Random().nextInt();
            try {
               processId = java.lang.management.ManagementFactory.getRuntimeMXBean().getName().hashCode();
            }
            catch ( Throwable t ){
            }

            ClassLoader loader = ObjectId.class.getClassLoader();
            int loaderId = loader != null ? System.identityHashCode(loader) : 0;

            StringBuilder sb = new StringBuilder();
            sb.append(Integer.toHexString(processId));
            sb.append(Integer.toHexString(loaderId));
            processPiece = sb.toString().hashCode() & 0xFFFF;
         }

         GENMACHINE = machinePiece | processPiece;
      }
      catch ( Exception e ){
         throw new RuntimeException(e );
      }
   }
}

