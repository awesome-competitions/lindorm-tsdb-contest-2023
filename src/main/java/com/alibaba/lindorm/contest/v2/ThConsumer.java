package com.alibaba.lindorm.contest.v2;

import java.io.IOException;

public interface ThConsumer<A, B, C> {
        void accept(A a, B b, C c) throws IOException;
    }