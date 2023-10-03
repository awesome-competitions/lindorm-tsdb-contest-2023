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

/**
 * An interface providing common functionality for <code>BitBuffer</code>s
 * and <code>VIntBuffers</code>.  We need the interface because we want to
 * finalize as much of the internals of the classes as we can for
 * performance reasons.
 */
public interface IntEncoder  {

    /**
     * Gets the number of bytes that it will take to write the data to a
     * file.
     */
    public int getNBytes();

    /**
     * Encodes an integer using the vInt encoding.
     */
    public int byteEncode(int n);

    /**
     * Decodes a vInt encoded integer.
     */
    public int byteDecode();

} // IntEncoder