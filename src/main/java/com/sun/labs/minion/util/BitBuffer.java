/*
 * Copyright 2007-2008 Sun Microsystems, Inc. All Rights Reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER
 *
 * This code is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2
 * only, as published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License version 2 for more details (a copy is
 * included in the LICENSE file that accompanied this code).
 *
 * You should have received a copy of the GNU General Public License
 * version 2 along with this work; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA
 *
 * Please contact Sun Microsystems, Inc., 16 Network Circle, Menlo
 * Park, CA 94025 or visit www.sun.com if you need additional
 * information or have any questions.
 */

package com.sun.labs.minion.util;

import java.io.DataInput;
import java.io.DataOutput;

import java.util.Arrays;
import java.util.logging.Logger;

/**
 * The BitBuffer class is used to represent a string of bits.  We're not
 * using a java.util.BitSet because it doesn't support operations, like
 * append, which we would like to have and it doesn't think that zeros at
 * the end of the buffer are significant (they are for us.)  BitSet has a
 * solid basis implementation, however, so we'll use it (we can't extend
 * the class because too many variables we need are private.)
 *
 * <p>
 *
 * The basic idea: An array of ints (called bits) is maintained to store
 * the bits.  Each int in this array is called a unit. We store bits
 * starting at unit 0, bit 0, which is the least-significant bit of the
 * int stored in bits[0].  Bits are stored from the least-significant
 * position to the most-significant position in each unit runnning from 0
 * to the size of the bits array.  An example:
 *
 * <PRE>
 *              Direction Units filled
 * -------------------------------------------->
 * ----------------------+----------------------+
 *         Unit 0        |         Unit 1       |
 * 3                    0|3                    0|
 * 2         ...        0|2          ...       0|
 * ----------------------+----------------------+
 * <---------------------|<---------------------|
 * Direction bits filled |Direction bits filled |
 * </PRE>
 *
 * <p>
 *
 * A "bit index" is simply a number giving a specific bit position in a
 * BitBuffer, so, for example, a bit index of 69 indicates the 5th
 * least-siginificant bit in bits[2] (i.e, bit 5 in bits[2]).
 *
 * <p>
 *
 * A "bit position" is a position within a unit, i.e., a number between 0
 * and 31.  So, the bit index 69 has bit position 5.
 *
 * <p>
 *
 * There are three numbers that describe the size of the array and where
 * bits are to be written to or read from: currUnit, which points to the
 * unit where the next bit will be written, currBit, which points to the
 * next bit to be set in the current unit, and head, which contains the bit
 * index of the next bit to be read from the head of the array.  All three
 * numbers are indexed from 0.
 *
 * <p>
 *
 * In general, we expect that bits will be read from the head of the
 * BitBuffer and written to the end of the BitBuffer.  BitSet provides
 * methods for setting and clearing a specific bit, which we will use with
 * some modification.
 *
 * <p>
 *
 * We also provide methods for writing and reading BitBuffers.  BitBuffers
 * can be written to any output stream that implements the
 * <code>DataOutput</code> interface.  A buffer is written in the following
 * manner:
 *
 * <ul>
 * <li> An int is written (as 4 bytes) containing the length of the buffer
 * in bits.
 * <li> The bits are written using an integral number of ints.
 * </ul>
 *
 * <p>
 *
 * Buffers can be read from any stream that implements the
 * <code>DataInput</code> interface.
 *
 * <p>
 *
 * The methods in the class have been synchronized where appropriate to
 * prevent overlapping writes, but you probably don't want to use the same
 * instance in different threads without some other synchronization method.
 * But hey, it's your funeral.
 *
 */
public class BitBuffer implements Cloneable, IntEncoder {

    /*
     * Currently a unit is an int, which consists of 32 bits, requiring 5
     * address bits.
     */
    protected final static int 	ADDRESS_BITS_PER_UNIT = 5;
    protected final static int 	BITS_PER_UNIT 	      = 1 << ADDRESS_BITS_PER_UNIT;
    protected final static int 	BIT_INDEX_MASK 	      = BITS_PER_UNIT - 1;
    protected final static int  FULL_UNIT 	      = 0xffffffff;

    private static Logger logger = Logger.getLogger(BitBuffer.class.getName());

    /**
     * The bits in this BitSet.  The ith bit is stored in bits[i/64] at
     * bit position i % 32 (where bit position 0 refers to the least
     * significant bit and 31 refers to the most significant bit).
     *
     */
    protected int bits[];

    /**
     * The unit where we will write the next bit.
     */
    protected int currUnit = 0;

    /**
     * The current bit in the last unit.  This is the bit that will be
     * written next.
     */
    protected int currBit = 0;

    /**
     * A pointer to the head of the current Buffer.
     */
    protected int head = 0;

    /**
     * A mark that we can reset to.
     */
    protected int markPoint = 0;

    /**
     * Given a bit index return unit index containing it.
     */
    protected final static int unitIndex(int bitIndex) {
        return bitIndex >>> ADDRESS_BITS_PER_UNIT;
    }

    /**
     * Given a bit index, return a unit that masks that bit in its unit.
     */
    protected final static int bit(int bitIndex) {
        return 1 << (bitIndex & BIT_INDEX_MASK);
    }

    /**
     * Find the position in a unit of a given bit index.
     *
     * @return the position in a unit of the given bit index.
     */
    protected final static int bitPos(int bitIndex) {
        return bitIndex & BIT_INDEX_MASK;
    }

    /**
     * Get the index of currBit.
     */
    public final int getCurrBitIndex() {
        return (currUnit << ADDRESS_BITS_PER_UNIT) + currBit;
    }

    /**
     * Get the number of bytes that it will take to store this
     * <code>BitBuffer</code> in a file.
     */
    public int getNBytes() {
        if(currBit > 0) {
            return((currUnit * 4) + 4);
        } else {
            return currUnit * 4;
        }
    }

    /**
     * Gets the total number of bytes that it will take to write this
     * buffer into a file.  This number includes the four bytes that the
     * length of the buffer will take.
     *
     * @return the number of bytes that this bit buffer will occupy when
     * written to a file.
     */
    public int getWBytes() {
        return getNBytes() + 4;
    }

    /**
     * Creates a new bit buffer. All bits are initially <code>false</code>.
     */
    public BitBuffer() {
        this(1024);
    }

    /**
     * Creates a bit buffer whose initial size is large enough to explicitly
     * represent bits with indices in the range <code>0</code> through
     * <code>nbits-1</code>. All bits are initially <code>false</code>. 
     *
     * @param     nbits   the initial size of the bit set.
     * @exception NegativeArraySizeException if the specified initial size
     *               is negative.
     */
    public BitBuffer(int nbits) {
        /* nbits can't be negative; size 0 is OK */
        if (nbits < 0) {
            throw new NegativeArraySizeException(Integer.toString(nbits));
        }

        bits = new int[(unitIndex(nbits-1) + 1)];
    }

    /**
     * Creates a bit buffer whose data is initialized by the bit buffer
     * given as a parameter.
     * @param b The bit buffer to use.
     */
    public BitBuffer(BitBuffer b) {
        bits = new int[b.bits.length];
        System.arraycopy(b.bits, 0, bits, 0, b.bits.length);
        head = b.head;
        currBit = b.currBit;
        currUnit = b.currUnit;
    }

    /**
     * Creates a bit buffer whose data shares the data of the bit buffer
     * given as a parameter. Useful for shared readers. (pmartin 8 jan 03)
     * @param resetp true resets the read pointer in the new BB
     * @param b The bit buffer to share the data from..
     */
    public BitBuffer(boolean resetp, BitBuffer b) {
        bits = b.bits;
        if (resetp) head = 0;
        else head = b.head;
        currBit = b.currBit;
        currUnit = b.currUnit;
    }

    /**
     * Creates a bit buffer by reading it from the provided stream.
     *
     * @param di The stream to read from.
     * @throws java.io.IOException if there is an error during reading.
     */
    public BitBuffer(DataInput di) throws java.io.IOException {
        read(di);
    }

    /**
     * Creates a bit buffer from an array of bytes.  This array of bytes
     * must have the same structure as the array that is constructed in the
     * <code>write</code> method.  The first four bytes of the array must
     * consist of the length of the buffer in bits.  The remaining bytes
     * encode the integers of the actual data.
     *
     * @param bytes The array of bytes to convert to a
     * <code>BitBuffer</code>
     * @param offset The offset in the array to start at.
     */
    public BitBuffer(byte[] bytes, int offset) {

        //
        // The first four bytes encode the index of the last bit in the
        // buffer.
        int lastBit =
                ((int) (bytes[offset++] & 0xFF) << 24) |
                        ((int) (bytes[offset++] & 0xFF) << 16) |
                        ((int) (bytes[offset++] & 0xFF) << 8) |
                        (int) (bytes[offset++] & 0xFF);

        //
        // Given the last bit index, we can now set up our variables.
        currBit  = bitPos(lastBit + 1);
        currUnit = unitIndex(lastBit + 1);
        head 	 = 0;
        bits 	 = new int[currUnit+1];

        //
        // Figure out how many bytes that will take to encode.
        int lastUnit = unitIndex(lastBit);

        //
        // Make our array of units.
        for(int i = 0; i <= lastUnit; i++) {
            bits[i] =
                    ((int) (bytes[offset++] & 0xFF) << 24) |
                            ((int) (bytes[offset++] & 0xFF) << 16) |
                            ((int) (bytes[offset++] & 0xFF) << 8) |
                            (int) (bytes[offset++] & 0xFF);
        }
    }

    public BitBuffer(String b) {
        this(b.length());
        for(int i = 0; i < b.length(); i++) {
            char c = b.charAt(i);
            if(c == '1') {
                set(i);
            }
        }
        currBit = bitPos(b.length());
        currUnit = unitIndex(b.length());
    }

    /**
     * Creates an array of <code>BitBuffer</code>s from the provided array
     * of bytes.  The idea is that if we've read multiple buffers from a
     * file, this method can convert them into a list of buffers useful for
     * things like iterating through postings files.
     */
    public static void createBuffers(BitBuffer[] buffers, byte[] bytes,
                                     int[] offsets) {

        for(int i = 0; i < offsets.length; i++) {

            int offset = offsets[i];

            if(offset >= 0) {

                BitBuffer buff = buffers[i];


                //
                // The first four bytes encode the index of the last bit in the
                // buffer.
                int lastBit =
                        ((int) (bytes[offset++] & 0xFF) << 24) |
                                ((int) (bytes[offset++] & 0xFF) << 16) |
                                ((int) (bytes[offset++] & 0xFF) << 8) |
                                (int) (bytes[offset++] & 0xFF);

                //
                // Given the last bit index, we can now set up our variables.
                buff.currBit  = bitPos(lastBit + 1);
                buff.currUnit = unitIndex(lastBit + 1);
                buff.head     = 0;

                if(buff.bits.length < buff.currUnit+1) {
                    buff.bits = new int[buff.currUnit+1];
                }

                //
                // Figure out how many bytes that will take to encode.
                int lastUnit = unitIndex(lastBit);

                //
                // Fill our array of units.
                for(int j = 0; j <= lastUnit; j++) {
                    buff.bits[j] =
                            ((int) (bytes[offset++] & 0xFF) << 24) |
                                    ((int) (bytes[offset++] & 0xFF) << 16) |
                                    ((int) (bytes[offset++] & 0xFF) << 8) |
                                    (int) (bytes[offset++] & 0xFF);
                }
            }
        }
    }

    /**
     * Cloning this <code>BitBuffer</code> produces a new
     * <code>BitBuffer</code> that is equal to it.  The clone of the bit
     * set is another bit set that has exactly the same bits set to
     * <code>true</code> as this bit set and the same current size.
     * <p>Overrides the <code>clone</code> method of <code>Object</code>.
     *
     * @return a clone of this bit buffer.
     */
    public Object clone() {
        BitBuffer result = null;
        try {
            result = (BitBuffer) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError();
        }

        result.bits = (int[]) bits.clone();
        return result;
    }

    /**
     * Share a <code>BitBuffer</code> among multiple readers.  There is a
     * strong assumption for this case that there will be no writers!  This
     * will be most useful for multiple iterators through a compressed
     * inverted file entry, where we don't wish to duplicate the array.
     *
     * @param shared The <code>BitBuffer</code> that we will share.
     * @return A <code>BitBuffer</code> sharing that data.
     *
     */
    public static BitBuffer share(BitBuffer shared) {
        BitBuffer 	b = new BitBuffer(1);
        b.bits 		  = shared.bits;
        b.currBit 	  = shared.currBit;
        b.currUnit 	  = shared.currUnit;
        return b;
    }

    //
    // Ensures that the BitBuffer can hold enough units.
    protected void ensureCapacity(int unitsRequired) {
        if (bits.length <= unitsRequired) {
            /* Allocate larger of doubled size or required size */
            int request   = Math.max(2 * bits.length, unitsRequired+1);
            int newBits[] = new int[request];
            System.arraycopy(bits, 0, newBits, 0, bits.length);
            bits = newBits;
        }
    }


    /**
     * Get the length of the bit string.
     *
     * @return the length of the bit string.
     */
    public int length() {

        //
        // The length of the bit string we're handling is the current bit
        // index minus the number of bits we've read off the front of the
        // buffer.
        return getCurrBitIndex() - head;
    }

    /**
     * Sets the bit specified by the index to <code>true</code>.  If the
     * bit that we set is further out than the current bit index, then
     * reset the position to just after the bit set.
     * @param bitIndex the bit index to set.
     */
    public void set(int bitIndex) {

        if(head > 0) {
            bitIndex += head;
        }

        int unitI = unitIndex(bitIndex);
        ensureCapacity(unitI+1);
        bits[unitI] |= bit(bitIndex);

        //
        // If the bitIndex we want to set is past the current point where
        // we're inserting bits, then move the pointer.
        if(bitIndex >= getCurrBitIndex()) {
            currBit = bitPos(++bitIndex);
            currUnit = unitIndex(bitIndex);
        }
    }


    /**
     * Test whether a given bit is true or false.
     *
     * @param bitIndex the index of the bit to test.
     * @return true if the bit is 1, false if it is 0
     */
    public boolean test(int bitIndex) {

        if(head > 0) {
            bitIndex += head;
        }

        int unitI = unitIndex(bitIndex);

        //
        // If it's past the end of the string, it must be 0.
        if(unitI > currUnit) {
            return false;
        }

        if((bits[unitI] & bit(bitIndex)) != 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Push a bit onto the end of the buffer.
     */
    public void push(boolean newbit) {
        push(newbit, 1);
    }

    /**
     * Push a bit onto the end of the buffer n times.
     *
     * @param newbit The bit to push.
     * @param n The number of times to push it.
     */
    public void push(boolean newbit, int n) {

        if(n == 0) {
            return;
        }

        int cBI = getCurrBitIndex();

        ensureCapacity(unitIndex(cBI+n)+1);

        //
        // We'll only do the heavy lifting when we want to push 1s.
        if(newbit) {
            int bitsInCurr = BITS_PER_UNIT - currBit;

            //
            // Will all bits fit in the current unit?
            if(n < bitsInCurr) {

                //
                // They do.  Or them in.
                bits[currUnit] |= (((1 << n) - 1) << currBit);

            } else {

                //
                // Fill up the current unit.
                bits[currUnit++] |= (FULL_UNIT << currBit);
                int left = n - bitsInCurr;

                int fullUnits = left >>> ADDRESS_BITS_PER_UNIT;

                //
                // Fill em up.
                for(int i = 0; i < fullUnits; i++) {
                    bits[currUnit++] = FULL_UNIT;
                }

                //
                // Make a mask for that unit and or it in.
                bits[currUnit] = ((1 << (left & BIT_INDEX_MASK)) - 1);
            }
        }

        currUnit = unitIndex(cBI+n);
        currBit  = bitPos(cBI+n);
    }

    /**
     * Pop a bit from the head of the current buffer.  Modifies the buffer
     * by removing the bit.
     *
     * @return true if the bit at the head of the buffer is 1.
     */
    public boolean pop() {

        boolean bit = peek();
        head++;
        return bit;
    }

    /*
     * Peek at the bit at the head of the currrent buffer.  Does not modify
     * the buffer.
     *
     * @return true if the bit at the head of the buffer is 1.
     */
    public boolean peek() {

        int 	unit = unitIndex(head);
        int 	mask = bit(head);

        if((bits[unit] & mask) != 0) {
            return true;
        }
        return false;
    }

    /**
     * Counts the number of bits of the given type in the buffer.
     *
     * @param b Whether we want to count <code>true</code> or
     * <code>false</code>.
     */
    public int countBits(boolean b) {
        if(b) {
            return countTrue();
        } else {
            return length() - countTrue();
        }
    }

    /**
     * Counts the number of on bits in the buffer.
     */
    protected int countTrue() {
        int ret = 0;
        int e = getCurrBitIndex();
        for(int i = head, j = bit(head), k = unitIndex(head), unit = bits[k] ;
            i < e; i++) {

            if((j & unit) != 0) {
                ret++;
            }

            //
            // Shift for the next bit.
            if(j == 0x80000000) {
                j = 0x1;
                unit = bits[++k];
            } else {
                j <<= 1;
            }
        }
        return ret;
    }

    /**
     * Clears out a bit buffer entirely.
     */
    public final void clear() {

        //
        // Clear using our array of zeros.
        if(bits.length < ca.length) {
            System.arraycopy(ca, 0, bits, 0, bits.length);
        } else {
            int n = bits.length / ca.length;
            int s = 0;
            for(int i = 0; i < n; i++, s += ca.length) {
                System.arraycopy(ca, 0, bits, s, ca.length);
            }
            int r = bits.length % ca.length;
            if(r > 0) {
                System.arraycopy(ca, 0, bits, s, r);
            }
        }
        head     = 0;
        currBit  = 0;
        currUnit = 0;
    }

    /**
     * Does a quick clear out.
     */
    public void quickClear() {
        head     = 0;
        currBit  = 0;
        currUnit = 0;
    }

    /**
     * Skip the given number of bits.
     *
     * @param n The number of bits to skip.
     */
    public void skip(int n) {
        head += n;
    }

    /**
     * Seeks to the given bit index.  Subsequent decodes will occur from
     * this point.
     *
     * @param i The index to set the head to.
     */
    public void seek(int i) {
        head = i;
    }

    /**
     * Tells where the read head currently is.
     *
     * @return the bit index for the current read position.
     */
    public int tell() {
        return head;
    }

    /**
     * Find the positon of the first bit matching b in the unit held in
     * unit, starting from position start.
     *
     * @return the position of the bit, or -1 if there is no such bit.
     */
    public static final int findFirstInUnit(boolean b, int unit, int start) {


        //
        // If we're looking for the first zero, just complement and look for
        // the first one.
        if(!b) {
            unit = ~unit;
        }

        //
        // Shift down to the bits we're interested in.
        unit >>>= start;

        //
        // This is an unrolled binary search for the rightmost bit set in the
        // unit.
        if((unit & 0x0000ffff) != 0) {
            if((unit & 0x000000ff) != 0) {
                if((unit & 0x0000000f) != 0) {
                    if((unit & 0x00000003) != 0) {
                        if((unit & 0x00000001) != 0) {
                            return 0;
                        } else {
                            return 1;
                        }
                    } else {
                        if((unit & 0x00000004) != 0) {
                            return 2;
                        } else {
                            return 3;
                        }
                    }
                } else {
                    if((unit & 0x00000030) != 0) {
                        if((unit & 0x00000010) != 0) {
                            return 4;
                        } else {
                            return 5;
                        }
                    } else {
                        if((unit & 0x00000040) != 0) {
                            return 6;
                        } else {
                            return 7;
                        }
                    }
                }
            } else {
                if((unit & 0x00000f00) != 0) {
                    if((unit & 0x00000300) != 0) {
                        if((unit & 0x00000100) != 0) {
                            return 8;
                        } else {
                            return 9;
                        }
                    } else {
                        if((unit & 0x00000400) != 0) {
                            return 10;
                        } else {
                            return 11;
                        }
                    }
                } else {
                    if((unit & 0x00003000) != 0) {
                        if((unit & 0x00001000) != 0) {
                            return 12;
                        } else {
                            return 13;
                        }
                    } else {
                        if((unit & 0x00004000) != 0) {
                            return 14;
                        } else {
                            return 15;
                        }
                    }
                }
            }
        } else {
            if((unit & 0x00ff0000) != 0) {
                if((unit & 0x000f0000) != 0) {
                    if((unit & 0x00030000) != 0) {
                        if((unit & 0x00010000) != 0) {
                            return 16;
                        } else {
                            return 17;
                        }
                    } else {
                        if((unit & 0x00040000) != 0) {
                            return 18;
                        } else {
                            return 19;
                        }
                    }
                } else {
                    if((unit & 0x00300000) != 0) {
                        if((unit & 0x00100000) != 0) {
                            return 20;
                        } else {
                            return 21;
                        }
                    } else {
                        if((unit & 0x00400000) != 0) {
                            return 22;
                        } else {
                            return 23;
                        }
                    }
                }
            } else {
                if((unit & 0x0f000000) != 0) {
                    if((unit & 0x03000000) != 0) {
                        if((unit & 0x01000000) != 0) {
                            return 24;
                        } else {
                            return 25;
                        }
                    } else {
                        if((unit & 0x04000000) != 0) {
                            return 26;
                        } else {
                            return 27;
                        }
                    }
                } else {
                    if((unit & 0x30000000) != 0) {
                        if((unit & 0x10000000) != 0) {
                            return 28;
                        } else {
                            return 29;
                        }
                    } else {
                        if((unit & 0x40000000) != 0) {
                            return 30;
                        } else if((unit & 0x80000000) != 0) {
                            return 31;
                        } else {
                            return -1;
                        }
                    }
                }
            }
        }
    }

    /**
     * Find the first instance of a bit matching the bit passed in.
     *
     * @return the number of bits from the head of the buffer to that bit,
     * or -1 if there is no such bit.
     */
    public final int findFirst(boolean b) {

        int 	headUI = unitIndex(head);
        int 	steps  = 0;

        //
        // Check first unit, if necessary.
        if(head > 0) {

            steps = findFirstInUnit(b, bits[headUI], bitPos(head));

            //
            // If we found such a bit, return it's position.
            if(steps >= 0) {
                return steps;
            }

            steps = BITS_PER_UNIT - bitPos(head);

            headUI++;
        }

        //
        // No such luck.  Look units full of the opposite bit.
        int 	full  = 0;
        int 	nFull = 0;

        if(!b) {
            full = FULL_UNIT;
        }

        for(; headUI <= currUnit && bits[headUI] == full;
            headUI++, steps += BITS_PER_UNIT);

        //
        // Check this unit.

        if(headUI <= currUnit) {
            steps += findFirstInUnit(b, bits[headUI], 0);
        }

        //
        // Return the position of the bit.
        return steps;

    }

    /**
     * Calculate the floor of the base 2 log of an integer n.  We're using
     * this rather than the library routines because it's 3-4 times faster.
     * The floor of the log of an integer is simply the index of the
     * left-most 1 bit in the number.
     *
     * <p>
     *
     * This is implemented as an unrolled binary search and is therefore
     * totally out of control (but hopefully fast.)
     *
     * @param n The int to calculate flLog2 for 
     * @return The floor of the log to the base 2 of the number.
     */
    public static final int flLog2(int n) {

        if(n <= 0) {
            throw new ArithmeticException("Negative number in log: " +
                    n);
        }

        //
        // This is an unrolled binary search for the leftmost bit.
        if((n & 0xffff0000) != 0) {
            if((n & 0xff000000) != 0) {
                if((n & 0xf0000000) != 0) {
                    if((n & 0xc0000000) != 0) {
                        if((n & 0x80000000) != 0) {
                            return 31;
                        } else {
                            return 30;
                        }
                    } else {
                        if((n & 0x20000000) != 0) {
                            return 29;
                        } else {
                            return 28;
                        }
                    }
                } else {
                    if((n & 0x0c000000) != 0) {
                        if((n & 0x08000000) != 0) {
                            return 27;
                        } else {
                            return 26;
                        }
                    } else {
                        if((n & 0x02000000) != 0) {
                            return 25;
                        } else {
                            return 24;
                        }
                    }
                }
            } else {
                if((n & 0x00f00000) != 0) {
                    if((n & 0x00c00000) != 0) {
                        if((n & 0x00800000) != 0) {
                            return 23;
                        } else {
                            return 22;
                        }
                    } else {
                        if((n & 0x00200000) != 0) {
                            return 21;
                        } else {
                            return 20;
                        }
                    }
                } else {
                    if((n & 0x000c0000) != 0) {
                        if((n & 0x00080000) != 0) {
                            return 19;
                        } else {
                            return 18;
                        }
                    } else {
                        if((n & 0x00020000) != 0) {
                            return 17;
                        } else {
                            return 16;
                        }
                    }
                }
            }
        } else {
            if((n & 0x0000ff00) != 0) {
                if((n & 0x0000f000) != 0) {
                    if((n & 0x0000c000) != 0) {
                        if((n & 0x00008000) != 0) {
                            return 15;
                        } else {
                            return 14;
                        }
                    } else {
                        if((n & 0x00002000) != 0) {
                            return 13;
                        } else {
                            return 12;
                        }
                    }
                } else {
                    if((n & 0x00000c00) != 0) {
                        if((n & 0x00000800) != 0) {
                            return 11;
                        } else {
                            return 10;
                        }
                    } else {
                        if((n & 0x00000200) != 0) {
                            return  9;
                        } else {
                            return  8;
                        }
                    }
                }
            } else {
                if((n & 0x000000f0) != 0) {
                    if((n & 0x000000c0) != 0) {
                        if((n & 0x00000080) != 0) {
                            return  7;
                        } else {
                            return  6;
                        }
                    } else {
                        if((n & 0x00000020) != 0) {
                            return  5;
                        } else {
                            return  4;
                        }
                    }
                } else {
                    if((n & 0x0000000c) != 0) {
                        if((n & 0x00000008) != 0) {
                            return  3;
                        } else {
                            return  2;
                        }
                    } else {
                        if((n & 0x00000002) != 0) {
                            return  1;
                        } else {
                            return  0;
                        }
                    }
                }
            }
        }
    }

    /**
     * Calculates the floor of the base 2 log for a long.
     */
    public final static int flLog2(long n) {

        if(n <= 0) {
            throw new ArithmeticException("Negative number in log: " +
                    n);
        }

        int a = (int) ((n >>> 32) & 0xffffffff);
        if(a == 0) {
            a = (int) (n & 0xffffffff);
            if(a < 0) {
                return 32;
            } else {
                return flLog2(a);
            }
        } else {
            return 32 + flLog2(a);
        }
    }

    /**
     * Reset the head to point at the start of bits array.
     */
    public void reset() {
        head = 0;
    }

    /**
     * Encode a positive integer directly, using a given number of
     * bits.
     *
     * @param n The number to encode.
     * @param nBits The number of bits to use in the encoding.
     * @return The number of bits used in the encoding.
     */
    public final int directEncode(int n, int nBits) {

        //
        // No bits means no encoding.
        if(nBits == 0) {
            return 0;
        }

        if(n > maxIntPerBits[nBits]) {
            logger.severe("Unable to encode " + n + " in " + nBits);
            throw new ArithmeticException("Attempt to encode " + n + " in " +
                    nBits + " bits");
        }

        //
        // Make sure there's enough room.
        int cBI = getCurrBitIndex();
        ensureCapacity(unitIndex(cBI+nBits+1));

        //
        // How many bits are in the current unit?
        int bitsInCurr = BITS_PER_UNIT - currBit;

        //
        // Put bits in the current unit.
        bits[currUnit] |= (n << currBit);

        //
        // Put any remaining bits in the next unit.
        if(nBits > bitsInCurr) {
            bits[currUnit+1] = (n >>> bitsInCurr);
        }

        currBit  = bitPos(cBI+nBits);
        currUnit = unitIndex(cBI+nBits);

        //
        // Get rid of crud beyond the current bit in our unit.
        bits[currUnit] &= (1 << currBit) - 1;
        return nBits;
    }

    /**
     * Encodes a long directly in the given number of bits.
     */
    public final int directEncode(long n, int nBits) {
        if(nBits > 32) {

            //
            // Encode the lower 32 bits, then the remaining upper bits.
            directEncode((int) (n & 0xffffffff), 32);
            directEncode((int) ((n >>> 32) & 0xffffffff), nBits - 32);

        } else {
            directEncode((int) (n & 0xffffffff), nBits);
        }
        return nBits;
    }

    /**
     * Direct decodes a long.
     */
    public final long directDecodeLong(int nBits) {
        if(nBits <= 32) {
            return directDecode(nBits);
        }

        int l = directDecode(32);
        int h = directDecode(nBits - 32);
        return ((long) (h & 0xffffffffL)) << 32 | (long) (l & 0xffffffffL);
    }

    /**
     * Decode a positive integer that was coded using a specific number of
     * bits.
     *
     * @param nBits The number of bits to use.
     * @return the decoded integer.
     */
    public final int directDecode(int nBits) {

        if(nBits == 0) {
            return 0;
        }

        int 	result 	   = 0;
        int 	headPos    = head & BIT_INDEX_MASK;
        int 	headUnit   = head >>> ADDRESS_BITS_PER_UNIT;
        int 	bitsInCurr = BITS_PER_UNIT - headPos;

        //
        // Get bits from the current unit.
        if(nBits == 32) {
            result = (bits[headUnit] >>> headPos);
        } else {
            result = ((bits[headUnit] >>> headPos) &
                    ((1 << nBits) - 1));
        }

        //
        // If we need more, get them from the next unit.
        if(nBits > bitsInCurr) {

            int diff = nBits - bitsInCurr;
            result |=
                    ((bits[headUnit+1] &
                            ((1 << diff) - 1)) << bitsInCurr);
        }

        head += nBits;
        return result;
    }

    /**
     * Unary encode an integer onto the end of the buffer.  The unary
     * encoding of an integer n is n-1 1 bits followed by a 0 bit.
     *
     * @param n The number to encode.
     * @return The number of bits used in the encoding.
     */
    public final int unaryEncode(int n) {
        push(true, n-1);
        push(false);

        //
        // Get rid of crud beyond the current bit in our unit.
        bits[currUnit] &= (1 << currBit) - 1;
        return n;
    }

    /**
     * Unary decode an int from the head of the BitBuffer.
     *
     * @return the decoded number.
     */
    public final int unaryDecode() {

        //
        // The position of the first zero is one less than the decoded
        // number.
        int pos = findFirst(false);
        pos++;
        head += pos;
        return pos;
    }

    /**
     * A "chunk" encoder, after an idea by Doug Cutting.  Sort of a
     * "chunkier" gamma encoding.  We unary encode the number of bytes that
     * the number can fit into, and then direct encode the number in that
     * number of bytes.  On average, we will have one or two comparisons,
     * compared to 6 for finding the floor of the base 2 log.
     *
     * <P>
     *
     * We could probably try the same trick at the nybble level, if we're
     * getting lots of numbers in the 1-128 range, which should show an
     * improvement over the byte encoding.
     */
    public final int chunkEncode(int n) {

        int 	b     = 0;
        int 	nBits = 0;
        if(n <= 0xff) {
            b = 1;
        } else if(n <= 0xffff) {
            b = 2;
        } else if(n < 0xffffff) {
            b = 3;
        } else if(n < 0xffffffff) {
            b = 4;
        }
        nBits += unaryEncode(b);
        nBits += directEncode(n, b * 8);
        return nBits;
    }

    /**
     * A chunk decoder.  Unary decode the number of bytes and then direct
     * decode the number
     */
    public final int chunkDecode() {
        int b = unaryDecode();
        return directDecode(b * 8);
    }

    /**
     * Encodes an integer in a byte-aligned fashion, using the minimal
     * number of bytes.  The basic idea: use the 7 lower order bits of a
     * byte to encode a number.  If the 8th bit is 0, then there are no
     * further bytes in this number.  If the 8th bit is one, then the next
     * byte continues the number.  Note that this means that a number that
     * would completly fill an int would take 5 bytes to encode.  The hope
     * is that we won't get numbers this big.
     */
    public final int byteEncode(int n) {

        //
        // Encode the first 7 bits.
        directEncode(n & 0x7F, 7);
        n >>>= 7;
        int 	nBits = 8;
        int 	full  = 0xffffffff;

        //
        // While there are bits left, keep encoding.
        while((n & full) != 0) {

            //
            // First, indicate that there are more bits using the eighth
            // bit in this byte.
            push(true);

            //
            // Encode the next 7 bits.
            directEncode(n & 0x7F, 7);
            n >>>= 7;
            nBits += 8;
            full >>>= 7;
        }

        push(false);
        return nBits;
    }

    /**
     * Decodes an integer stored using the byte encoding.
     *
     * @return the decoded integer.
     * @see #byteEncode
     */
    public final int byteDecode() {

        //
        // Where are we?
        int 	headPos  = head & BIT_INDEX_MASK;
        int 	headUnit = head >>> ADDRESS_BITS_PER_UNIT;
        int 	u 	 = 0;

        //
        // Get a single unit.
        if(headPos == 0) {
            u = bits[headUnit];
        } else {
            u = bits[headUnit++] >>> headPos;
            if(headUnit < bits.length) {
                u |= bits[headUnit] << (BITS_PER_UNIT - headPos);
            }
        }

        //
        // Get the first 7 bits.
        int result = u & 0x7f;

        //
        // Check if there is more coming.
        if((u & 0x80) != 0) {
            result |= ((u & 0x7f00) >>> 1);
            if((u & 0x8000) != 0) {
                result |= ((u & 0x7f0000) >>> 2);
                if((u & 0x800000) != 0) {
                    result |= ((u & 0x7f000000) >>> 3);
                    head += 32;
                } else {
                    head += 24;
                }
            } else {
                head += 16;
            }
        } else {
            head += 8;
        }
        return result;
    }

    /**
     * Gamma encode an int onto the end of the buffer.  For a positive
     * integer <em>n</em> if <em>fl</em>=floor(log2(<em>n</em>)), then the
     * gamma encoding of <em>n</em> is the unary encoding of <em>fl</em>+1
     * followed by the direct encoding of the difference
     * <em>n</em>-2<sup><em>fl</em></sup> in <em>fl</em> bits.
     *
     * @param n The int to encode
     * @return The number of bits used in the encoding.
     */
    public int gammaEncode(int n) {

        int fl = flLog2(n);

        //
        // Unary encode fl.
        unaryEncode(fl+1);

        //
        // Find the difference.
        int diff = n - (1 << fl);
        directEncode(diff, fl);
        return 2 * fl + 1;
    }

    /**
     * Gamma decode the head of the buffer.
     *
     * @return the decoded number.
     */
    public int gammaDecode() {

        int 	fl 	  = unaryDecode() - 1;
        int 	remainder = directDecode(fl);
        return (1 << fl) + remainder;
    }

    /**
     * Delta encode an int onto the end of the buffer.  For a positive
     * integer <em>n</em> if <em>fl</em>=floor(log2(<em>n</em>)), then the
     * delta encoding of <em>n</em> is the gamma encoding of <em>fl</em>+1,
     * followedby the direct encoding of the difference
     * <em>n</em>-2<sup><em>fl</em></sup> in <em>fl</em> bits.
     *
     * @param n The int to encode.
     * @return the number of bits used in the encoding.
     */
    public int deltaEncode(int n) {

        int 	fl    = flLog2(n);
        int 	nBits = gammaEncode(fl+1);

        //
        // Find the difference.
        int diff = n - (1 << fl);
        directEncode(diff, fl);
        return nBits+fl;
    }

    /**
     * Delta decode an int from the front of the buffer.
     *
     * @return the decoded int.
     */
    public int deltaDecode() {
        int 	fl 	  = gammaDecode() - 1;
        int 	remainder = directDecode(fl);
        return (1 << fl) + remainder;
    }

    /**
     * Encodes an array of characters using a modified UTF-8 encoding in a
     * machine independant manner.
     *
     * <P>
     *
     * First, we write the number of bytes in the encoding, using either
     * 10, 18, or 34 bits.  Following the length, each character of the
     * string is output, in sequence, using the UTF-8 encoding for the
     * character.
     *
     * <P>
     *
     * This code is based on that in {@link java.io.DataOutputStream}.
     *
     * @param a The array of characters to encode.
     * @return The number of bits used in the encoding.
     */
    public int encodeChars(char[] a) {
        return encodeChars(a, 0, a.length);
    }

    /**
     * Encodes a number of bytes in a manner suitable for use by
     * decodeChars. The number of bytes is encoded in 10, 16, or 34
     * bits. depending on the magnitude of the number.
     *
     * @param n The number of bytes to encode.
     * @return The number of bits used in the encoding.
     */
    public int encodeUTFLen(int n) {

        //
        // Now code the length onto our buffer.
        if(n <= 0xff) {
            push(false);
            push(false);
            directEncode(n, 8);
            return 10;
        }

        if(n > 0xff && n <= 0xffff) {
            push(true);
            push(false);
            directEncode(n, 16);
            return 18;
        }

        push(true);
        push(true);
        directEncode(n, 32);
        return 34;
    }

    /**
     * Encodes an array of characters using a modified UTF-8 encoding in a
     * machine independant manner.  We first encode the length of the
     * encoded characters, followed by the UTF-8 encodings for the
     * characters.
     *
     * @param a The array of characters to encode.
     * @param b The beginning offset in the array
     * @param e The exclusive ending offset in the array.
     * @return The number of bits used to encode the array.
     */
    public int encodeChars(char[] a, int b, int e) {

        BitBuffer 	temp   = new BitBuffer((e - b) * 8 +1);
        int 		utflen = encodeChars(a, b, e, temp);
        int 		nBits  = encodeUTFLen(utflen);

        //
        // Append the encoded data.
        append(temp);
        return (utflen * 8) + nBits;
    }

    /**
     * Encodes an array of characters using a modified UTF-8 encoding in a
     * machine independant manner.  This method does not encode the length
     * of the string onto the front of the buffer.  In order to do that you
     * must use the three argument version.  This method is intended for
     * cases where you want to encode characters without the final length
     * of the string begin known.
     *
     * <P>
     *
     * First, we write the number of bytes in the encoding, using either
     * 10, 18, or 34 bits.  Following the length, each character of the
     * string is output, in sequence, using the UTF-8 encoding for the
     * character.
     *
     * <P>
     *
     * This code is based on that in {@link java.io.DataOutputStream}.
     *
     * @param a The array of characters to encode.
     * @param b The beginning offset in the array
     * @param e The exclusive ending offset in the array.
     * @return The number bytes in the UTF encoding.
     * @see #encodeChars(char[], int, int)
     */
    public static int encodeChars(char[] a, int b, int e, BitBuffer buff) {

        int 	utfLen = 0;
        int 	c;

        for (int i = b; i < e; i++) {
            c = a[i];
            if ((c >= 0x0000) && (c <= 0x007F)) {
                buff.directEncode(c, 8);
                utfLen++;
            } else if (c > 0x07FF) {
                buff.directEncode((0xE0 | ((c >> 12) & 0x0F)), 8);
                buff.directEncode((0x80 | ((c >>  6) & 0x3F)), 8);
                buff.directEncode((0x80 | ((c >>  0) & 0x3F)), 8);
                utfLen += 3;
            } else {
                buff.directEncode((0xC0 | ((c >>  6) & 0x1F)), 8);
                buff.directEncode((0x80 | ((c >>  0) & 0x3F)), 8);
                utfLen += 2;
            }
        }

        return utfLen;
    }

    /**
     * Encodes a string using a modified UTF-8 encoding in a machine
     * independant manner.
     *
     * @param s The string to encode.
     * @return The number of bits used in the encoding.
     * @see #encodeChars
     */
    public int encodeUTF(String s) {

        //
        // Catch empty or null strings.
        if(s == null || s.length() == 0) {
            return encodeUTFLen(0);
        }

        return encodeChars(s.toCharArray(), 0, s.length());
    }

    /**
     * Encodes an array integers using a standard difference method.  The
     * integers will be returned in sorted order.
     */
    public int differenceEncodeArray(int n, int[] a) {
        if(n == 0) {
            return gammaEncode(1);
        }

        Arrays.sort(a, 0, n);
        int 	nBits = gammaEncode(n+1);
        int 	last  = 0;
        for(int i = 0; i < n; i++) {
            nBits += deltaEncode(a[i] - last + 1);
            last   = a[i];
        }
        return nBits;
    }

    /**
     * Decodes an array of integers using the standard difference method.
     */
    public int[] differenceDecodeArray() {
        int n = gammaDecode() - 1;
        if(n == 0) {
            return null;
        }

        int[] a = new int[n];
        int last = 0;
        for(int i = 0; i < n; i++) {
            a[i] = deltaDecode() - 1 + last;
            last = a[i];
        }
        return a;
    }

    /**
     * Encodes any Java int.
     *
     * @param n The int to encode.
     * @return The number of bits used to encode the number.
     */
    public int encodeInt(int n) {
        if(n < 0) {
            push(true);
            if(n == Integer.MIN_VALUE) {
                push(true);
                return 2;
            } else {
                push(false);
                return deltaEncode(-n) + 2;
            }
        }

        if(n == 0) {
            push(false);
            push(true);
            return 2;
        }

        push(false);
        push(false);
        return deltaEncode(n) + 2;
    }

    /**
     * Decodes any Java int.
     */
    public int decodeInt() {
        if(pop()) {
            if(pop()) {
                return Integer.MIN_VALUE;
            } else {
                return -deltaDecode();
            }
        }

        if(pop()) {
            return 0;
        }
        return deltaDecode();
    }

    /**
     * Encodes any Java long.
     */
    public int encodeLong(long n) {

        return encodeInt((int) (n >>> 32)) +
                encodeInt((int) n);
    }

    /**
     * Decodes any Java long.
     */
    public long decodeLong() {
        return (long) decodeInt() << 32 |
                (long) decodeInt();
    }

    /**
     * Append another BitBuffer to this one.
     *
     * @param b The <code>BitBuffer</code> to append.
     */
    public void append(BitBuffer b) {

        int 	ourLen 	 = length();
        int 	theirLen = b.length();

        if(theirLen == 0) {
            return;
        }

        //
        // Check whether we can just quickly array copy.
        int bH = bitPos(b.head);
        if(currBit == 0 && bH == 0) {

            int bHU    	= unitIndex(b.head);
            int nUnits 	= b.currUnit - bHU;
            if(b.currBit > 0) {
                nUnits++;
            }
            ensureCapacity(currUnit + nUnits + 1);
            System.arraycopy(b.bits, bHU, bits, currUnit, nUnits);
            currUnit = unitIndex(ourLen+theirLen);
            currBit  = bitPos(ourLen+theirLen);
        } else {

            //
            // Just copy the bits, and then fix the head pointer in the
            // other entry.
            int saveHead = b.head;
            copyNBitsFrom(b, theirLen);
            b.head = saveHead;
        }
    }

    /**
     * Copies n bits from the given buffer onto our buffer.
     *
     * @param b The <code>BitBuffer</code> to copy bits from.
     * @param n The number of bits to copy.
     */
    public void copyNBitsFrom(BitBuffer b, int n) {

        int cBI = getCurrBitIndex();
        ensureCapacity(unitIndex(cBI+n)+1);

        //
        // Where are we in the other buffer?
        int 	hP    = bitPos(b.head);
        int 	hU    = unitIndex(b.head);
        int 	nLeft = BITS_PER_UNIT - hP;

        //
        // How many bits are in to the left of the currBit pointer in this
        // buffer?
        int 	rightShift = BITS_PER_UNIT - currBit;

        //
        // How many units will we need to get from the other buffer.
        int nU = unitIndex(n);

        //
        // The bit position in the fractional unit.
        int nP = bitPos(n);

        //
        // An intermediary for a unit.
        int temp;

        //
        // Do the full units.
        for(int i = 0; i < nU; i++) {

            //
            // If the head pointer for the other buffer is in the middle, then
            // get the unit in two pieces.
            if(hP != 0) {
                temp  = b.bits[hU++] >>> hP;
                temp |= b.bits[hU] << nLeft;
            } else {
                temp = b.bits[hU++];
            }

            if(currBit != 0) {

                //
                // Our pointer is in the middle of a unit, so put this unit
                // across two adjacent units.
                bits[currUnit++] |= temp << currBit;
                bits[currUnit]    = temp >>> rightShift;
            } else {

                //
                // currBit's at the start of a unit, just put the new unit in
                // it.
                bits[currUnit++] = temp;
            }
        }

        //
        // Now, deal with the last unit, if there are bits in it.  First
        // case: all the bits are in the current unit of the other buffer.
        if(nP > 0) {
            if(nP <= nLeft) {

                //
                // We just want those bits, so mask them out.
                temp = (b.bits[hU] >>> hP) & ((1 << nP) - 1);

            } else {

                //
                // The bits are in two units.
                temp = (b.bits[hU++] >>> hP);
                temp |= ((b.bits[hU] & ((1 << (nP - nLeft)) - 1)) << nLeft);
            }

            //
            // Put the last unit into our buffer.
            bits[currUnit] |= (temp << currBit);
            if(nP > rightShift) {
                currUnit++;
                bits[currUnit] = (temp >>> rightShift);
            }
        }

        //
        // Fix our current bit and unit.
        currBit  = bitPos(cBI+n);
        currUnit = unitIndex(cBI+n);
        bits[currUnit] &= (1 << currBit) - 1;

        //
        // Remove the bits from the other buffer.
        b.head += n;
    }

    /**
     * Calculate the bitwise or of this buffer and another.
     *
     * @param r The <code>BitBuffer</code> to or with this one.
     */
    public void or(BitBuffer r) {

        //
        // Make sure we're big enough.
        ensureCapacity(unitIndex(Math.max(length(), r.length()))+1);

        //
        // Do the run.
        for(int i = 0; i <= r.currUnit; i++) {
            bits[i] |= r.bits[i];
        }

        if(currUnit == r.currUnit) {
            if(currBit < r.currBit) {
                currBit = r.currBit;
            }
        } else if(currUnit < r.currUnit) {
            currUnit = r.currUnit;
            currBit = r.currBit;
        }

        //
        // Get rid of crud beyond the current bit in our unit.
        bits[currUnit] &= (1 << currBit) - 1;
    }

    /**
     * Calculate the bitwise and of this buffer and another.
     *
     * @param r The <code>BitBuffer</code> to and with this one.
     */
    public void and(BitBuffer r) {

        //
        // Make sure we're big enough.
        ensureCapacity(unitIndex(Math.max(length(), r.length()))+1);

        int i;
        //
        // Do the run.
        for(i = 0; i <= r.currUnit; i++) {
            bits[i] &= r.bits[i];
        }

        if(currUnit == r.currUnit) {
            if(currBit < r.currBit) {
                currBit = r.currBit;
            }
        } else if(currUnit < r.currUnit) {
            currUnit = r.currUnit;
            currBit = r.currBit;
        } else if(currUnit > r.currUnit) {

            //
            // If we're longer than the other one, then we need to set the
            // remaining units to 0.
            for(; i <= currUnit; i++) {
                bits[i] = 0;
            }
        }
    }

    /**
     * Calculate the bitwise xor of this buffer and another.
     *
     * @param r the <code>BitBuffer</code> to xor with this one.
     */
    public void xor(BitBuffer r) {

        //
        // Make sure we're big enough.
        ensureCapacity(unitIndex(Math.max(length(), r.length()))+1);

        for(int i = 0; i <= r.currUnit; i++) {
            bits[i] ^= r.bits[i];
        }

        if(currUnit == r.currUnit) {
            if(currBit < r.currBit) {
                currBit = r.currBit;
            }
        } else if(currUnit < r.currUnit) {
            currUnit = r.currUnit;
            currBit = r.currBit;
        }
    }

    /**
     * Calculate the  bitwise complement of this buffer.
     */
    public void complement() {

        //
        // Do everything but the last unit.
        for(int i = 0; i < currUnit; i++) {
            bits[i] = ~bits[i];
        }

        //
        // Only do the last unit if we need to.
        if(currBit > 0) {

            //
            // Mask out currBit bits from the last unit and complement only
            // those.
            bits[currUnit] =
                    ~(bits[currUnit] & ((1 << currBit) - 1));
        }
    }

    /**
     * Write this BitBuffer to another.
     */
    public void write(BitBuffer b) {
        b.bits[b.currUnit] = 0;
        bits[currUnit] &= (1 << currBit) - 1;
        b.directEncode(length() - 1, 32);
        b.append(this);
        b.ensureCapacity(b.currUnit+1);
        if(b.currBit > 0) {
            b.currBit = 0;
            b.currUnit++;
        }
    }

    /**
     * Write a buffer to a stream with the length.
     */
    public void write(DataOutput dout)
            throws java.io.IOException {
        write(dout, true);
    }

    /**
     * Write a <code>BitBuffer</code> to a class implementing
     * DataOuput. If the second parameter is true, then the length of the
     * buffer will be written out first as a 4 byte integer.
     *
     * <p>
     *
     * If the second parameter is false, then the length will not be
     * written out.  This option is meant to be used in situations where we
     * want to batch up writes to provide better performance.  The proviso
     * is the user <em>must</em> write the number of bits to read in to the
     * head of the string.  If this is not done, you <em>will not</em> be
     * able to read the <code>BitBuffer</code> back in.
     *
     * @param dout The <code>DataOutput</code> object to write to.
     * @param writeLength If true, the length of the buffer will be written
     * out first as a 32 bit number.
     * @exception java.io.IOException if the write fails.
     */
    public void write(DataOutput dout, boolean writeLength)
            throws java.io.IOException {

        //
        // Figure out the index of the last actual bit in the buffer, and
        // the unit that it's in.
        int 	lastBitIndex = length() - 1;
        int 	lastPos      = bitPos(lastBitIndex);
        int 	lastUnit     = unitIndex(lastBitIndex);

        //
        // Write the int representing the index of the last bit to the
        // stream, if we're supposed to.
        if(writeLength) {
            dout.writeInt(lastBitIndex);
        }

        //
        // If the last bit index is -1, then the buffer is empty and we can
        // quit.
        if(lastBitIndex == -1) {
            return;
        }

        //
        // Clear out any crud after the last bit.
        bits[currUnit] &= (1 << currBit) - 1;

        //
        // Determine how many bytes we need to write to get that many
        // bits.
        int nBytes = (lastUnit + 1) * 4;

        //
        // Allocate space for the necessary bytes.
        byte[] bytes = new byte[nBytes];

        int 	x 	 = 0;
        int 	b 	 = 0;
        int 	begIndex = 0;
        int 	endIndex = 0;

        //
        // Walk our array of units.
        for(int i = 0; i <= lastUnit; i++) {

            x = bits[i];

            //
            // We want the bytes to be in the array in the same order that
            // they are in the integer.
            bytes[b++] = (byte) ((x >>> 24) & 0xFF);
            bytes[b++] = (byte) ((x >>> 16) & 0xFF);
            bytes[b++] = (byte) ((x >>> 8) & 0xFF);
            bytes[b++] = (byte) (x & 0xFF);
        }

        //
        // Write the whole thing out.
        dout.write(bytes, 0, nBytes);
    }

    /**
     * Read a <code>BitBuffer</code> from an object that implements
     * DataInput.  This will allow us to use BitBuffers with random access
     * files.
     *
     * @param din The <code>DataInput</code> object to read from.
     * @exception java.io.IOException if the read fails.
     */
    public void read(DataInput din) throws java.io.IOException {

        //
        // First, read in the last bit index, so that we know how many
        // bytes to read.
        int 	lastBit = din.readInt();

        //
        // Given the last bit index, we can now set up our variables.
        currBit  = bitPos(lastBit + 1);
        currUnit = unitIndex(lastBit + 1);
        head 	 = 0;
        bits 	 = new int[currUnit+1];

        //
        // If the last bit was -1, the buffer is empty.
        if(lastBit == -1) {
            return;
        }

        //
        // Determine how many bytes we need to read to get that many bits,
        // and then read them.
        int 	lastUnit = unitIndex(lastBit);
        int 	nBytes 	 = (lastUnit + 1) * 4;
        byte[] bytes 	 = new byte[nBytes];

        din.readFully(bytes, 0, nBytes);

        //
        // Now, unpack the bytes into our array of units.
        int 	b = 0;
        for(int i = 0; i <= lastUnit; i++) {
            bits[i] =
                    ((int) (bytes[b++] & 0xFF) << 24) |
                            ((int) (bytes[b++] & 0xFF) << 16) |
                            ((int) (bytes[b++] & 0xFF) << 8) |
                            (int) (bytes[b++] & 0xFF);
        }
    }

    /**
     * Skips a bit buffer without reading it in.
     */
    public static void skip(DataInput in) throws java.io.IOException {

        int lastBit = in.readInt();

        //
        // If the last bit was -1, the buffer is empty.
        if(lastBit == -1) {
            return;
        }

        //
        // Determine how many bytes we need to skip and skip them.
        in.skipBytes((unitIndex(lastBit) + 1) * 4);
    }

    /**
     * Get the bits in a unit from right to left, put them in a string left
     * to right, starting at the given position.
     * @param unit the unit to process
     * @param pos the place to start.
     * @return A String cotaining the bits in the unit.
     */
    protected String unitToLogicalString(int unit, int pos) {

        StringBuffer logical = new StringBuffer(64);

        int temp = (bits[unit] >>> pos);

        for(int i = 0; i < BITS_PER_UNIT - pos; i++) {
            if((temp & 1) != 0) {
                logical.append('1');
            } else {
                logical.append('0');
            }
            temp >>>= 1;
        }

        return logical.toString();
    }

    /**
     * Build a string representation of an int with the bits in the right
     * order.
     *
     * @param n The number to represent
     */
    protected static String intToBinaryString(int n) {

        StringBuffer b = new StringBuffer(32);

        for(int i = 0; i < BITS_PER_UNIT; i++) {
            if(i % 4 == 0) {
                b.append("|");
            }
            if(n < 0) {
                b.append("1");
            } else {
                b.append("0");
            }
            n <<= 1;
        }

        return b.toString();
    }

    protected static String longToBinaryString(long n) {

        StringBuffer b = new StringBuffer(72);

        for(int i = 0; i < 64; i++) {
            if(i % 4 == 0) {
                b.append("|");
            }
            if(n < 0) {
                b.append("1");
            } else {
                b.append("0");
            }
            n <<= 1;
        }

        return b.toString();
    }

    /**
     * Print the bits in the buffer in the order in which they actually
     * occur.
     */
    public String toActualString() {

        StringBuffer b = new StringBuffer(currUnit * BITS_PER_UNIT);

        for(int i = 0; i <= currUnit; i++) {
            b.append(intToBinaryString(bits[i]));
            if(i < currUnit) {
                b.append('\n');
            }
        }

        return b.toString();
    }

    /**
     * Provide a logical string representation of the bit buffer.  A
     * logical representation shows the bits in the buffer from left
     * to right as you would expect them.
     *
     * @return the representation.
     */
    public String toLogicalString() {

        //
        // Empty bit buffer.
        if(length() == 0) {
            return "";
        }

        StringBuffer out = new StringBuffer(length());
        int currBitIndex = getCurrBitIndex();

        //
        // Mask out each bit between the head of the buffer and the current
        // insertion point.  Add 1s or 0s to the output buffer as
        // appropriate.
        for(int temp = head; temp < currBitIndex; temp++) {
            if((bits[unitIndex(temp)] & bit(temp)) != 0) {
                out.append("1");
            } else {
                out.append("0");
            }
        }

        return out.toString();
    }

    public String toString() {
        return toLogicalString() + " (" + length() +
                ", " + head + ", " + currUnit + ", " + currBit +
                ")";
    }

    /**
     * Some zeros we can use to clear out buffers.
     */
    protected static int[] ca =
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0};

    /**
     * The maximum number that can be encoded in a given number of bits.
     * Used to check directEncode input.
     */
    protected static int[] maxIntPerBits =
            {0, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192,
                    16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152,
                    4194304, 8388608, 16777216, 33554432, 67108864, 134217728,
                    268435456, 536870912, 1073741824, 2147483647, 2147483647};


    public static void main(String[] args) {

//        long s = System.currentTimeMillis();
//        BitBuffer bf = new BitBuffer(1000);
//        for (int i = 0; i < 1000_0000; i ++){
//            bf.directEncode(Long.MAX_VALUE, 64);
//            bf.directEncode(Integer.MAX_VALUE, 32);
//            bf.clear();
//        }
//        System.out.println(System.currentTimeMillis() - s);

        System.out.println(maxIntPerBits.length);

        BitBuffer bf = new BitBuffer(120);
        bf.directEncode(7, 3);
        bf.directEncode(15, 4);
        bf.directEncode(2,3);
        bf.directEncode(Long.MAX_VALUE, 64);
        bf.directEncode(Long.MIN_VALUE, 64);

        System.out.println(bf.directDecode(3));
        System.out.println(bf.directDecode(4));
        System.out.println(bf.directDecode(3));
        System.out.println(bf.directDecodeLong(64));
        System.out.println(bf.directDecodeLong(64));




    }
}