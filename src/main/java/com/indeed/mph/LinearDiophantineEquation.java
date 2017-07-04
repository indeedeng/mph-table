package com.indeed.mph;

import java.io.Serializable;

/**
 * Linear equation to represent SmartSerializer sizes in one unknown.
 *
 * @author alexs
 */
public class LinearDiophantineEquation implements Serializable {
    // TODO: allow multiple unknowns.
    private final long slope;
    private final long constant;

    LinearDiophantineEquation(final long slope, final long constant) {
        this.slope = slope;
        this.constant = constant;
    }

    public static LinearDiophantineEquation constantValue(final long constant) {
        return new LinearDiophantineEquation(0, constant);
    }

    public static LinearDiophantineEquation multipleOf(final long slope) {
        return new LinearDiophantineEquation(slope, 0);
    }

    public static LinearDiophantineEquation slopeIntercept(final long slope, final long constant) {
        return new LinearDiophantineEquation(slope, constant);
    }

    public LinearDiophantineEquation add(final LinearDiophantineEquation other) {
        if (other == null) {
            return this;
        }
        return new LinearDiophantineEquation(gcd(slope, other.slope), constant + other.constant);
    }

    public LinearDiophantineEquation repeat(final long n) {
        return new LinearDiophantineEquation(slope, constant * n);
    }

    public long getSlope() {
        return slope;
    }

    public long getConstant() {
        return constant;
    }

    public boolean isConstant() {
        return slope == 0;
    }

    public long apply(final long x) {
        return slope * x + constant;
    }

    public long solveFor(final long y) {
        return (y - constant) / (slope == 0 ? 1 : slope);
    }

    // shortcuts for solving with repeat(n)

    public long applyNth(final long x, final long n) {
        return slope * x + constant * n;
    }

    public long solveForNth(final long y, final long n) {
        return (y - constant * n) / (slope == 0 ? 1 : slope);
    }

    public String toString() {
        return "(" + slope + "*x + " + constant + ")";
    }

    public int hashCode() {
        return (int) (37 * slope + constant);
    }

    public boolean equals(final Object obj) {
        if (obj == null || !(obj instanceof LinearDiophantineEquation)) {
            return false;
        }
        final LinearDiophantineEquation lde = (LinearDiophantineEquation) obj;
        return slope == lde.getSlope() && constant == lde.getConstant();
    }

    public static long gcd(long a, long b) {
        a = Math.abs(a);
        b = Math.abs(b);
        while (b != 0) {
            final long b2 = a % b;
            a = b;
            b = b2;
        }
        return a;
    }
}
