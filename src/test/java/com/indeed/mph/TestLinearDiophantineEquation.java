package com.indeed.mph;

import org.junit.Test;

import static com.indeed.mph.LinearDiophantineEquation.constantValue;
import static com.indeed.mph.LinearDiophantineEquation.gcd;
import static com.indeed.mph.LinearDiophantineEquation.multipleOf;
import static com.indeed.mph.LinearDiophantineEquation.slopeIntercept;
import static org.junit.Assert.assertEquals;

public class TestLinearDiophantineEquation {

    @Test
    public void testEq() throws Exception {
        assertEquals(slopeIntercept(0, 1), constantValue(1));
        assertEquals(slopeIntercept(1, 0), multipleOf(1));
        assertEquals(slopeIntercept(1, 2), constantValue(2).add(multipleOf(1)));
        assertEquals(slopeIntercept(1, 200), constantValue(2).add(multipleOf(1)).repeat(100));
        assertEquals(slopeIntercept(3, 0), multipleOf(15).add(multipleOf(21)));
        assertEquals(slopeIntercept(3, 0), multipleOf(15).add(multipleOf(21)).repeat(2));
        assertEquals(slopeIntercept(1, 8), constantValue(8).add(multipleOf(1)));
        assertEquals(slopeIntercept(1, 8), multipleOf(1).add(constantValue(8)));
    }

    @Test
    public void testGcd() throws Exception {
        assertEquals(0, gcd(0, 0));
        assertEquals(1, gcd(0, 1));
        assertEquals(1, gcd(1, 0));
        assertEquals(1, gcd(1, 1));
        assertEquals(1, gcd(1, 2));
        assertEquals(1, gcd(2, 1));
        assertEquals(1, gcd(3, 5));
        assertEquals(2, gcd(2, 4));
        assertEquals(3, gcd(3, 9));
        assertEquals(3, gcd(9, 6));
        assertEquals(3, gcd(9, -6));
        assertEquals(3, gcd(-9, -6));
    }
}
