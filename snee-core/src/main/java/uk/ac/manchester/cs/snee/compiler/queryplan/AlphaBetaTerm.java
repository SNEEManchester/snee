/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * A single term in the Alpha Beta Expression.
 * @author Christian Brenninkmeijer and Ixent Galpin
 *
 */
public class AlphaBetaTerm {

	/** Coefficient */
	private double coefficient;
	/** Exponent of Alpha. */
	private double alphaExponent; 
	/** Exponent of Beta. */
	private double betaExponent;
	
	/** Format for showing decimal numbers */
    private final DecimalFormat df = new DecimalFormat("0.000000");

	/** Constant only constructor.
	 * @param theNumerator The constant part of term.
	 */
	public AlphaBetaTerm(final double theNumerator) {
		this(theNumerator, 1, 0, 0);
	}

	/**
	 * Constructor.
	 * @param theNumerator The Constant part of term.
	 * @param theAlphaExponent The Exponent for Alpha.
	 * @param theBetaExponent The Exponent for Beta.
	 */
	public AlphaBetaTerm(final double theNumerator, 
			final double theAlphaExponent, final double theBetaExponent) {
		this(theNumerator, 1, theAlphaExponent, theBetaExponent);
	}

	/**
	 * Constructor.
	 * @param theNumerator Numerator part of the Constant part of term.
	 * @param theDenominator Denominator part of the constant.
	 * @param theAlphaExponent The Exponent for Alpha.
	 * @param theBetaExponent The Exponent for Beta.
	 */
	public AlphaBetaTerm(final double theNumerator, 
			final double theDenominator, final double theAlphaExponent,
			final double theBetaExponent) {
		assert (theDenominator != 0);
		coefficient = (theNumerator/theDenominator);
		alphaExponent = theAlphaExponent;
		betaExponent = theBetaExponent;
	}

	/**
	 * Deep clone method.
	 * @return A deep copy of this term.
	 */
	public final AlphaBetaTerm clone() {
		return new AlphaBetaTerm(coefficient, 
			alphaExponent, betaExponent);
	}
	/**
	 * Adds a constant to this term.  Stores the result in this term.
	 * Only valid if this term is a constant 
	 * where both Alpha and Beta Exponent equal zero.
	 * @param constant Value to be added.
	 */
	protected final void add(final double constant) {
		assert (alphaExponent == 0);
		assert (betaExponent == 0);
		this.coefficient += constant;
	}
	
	/**
	 * Adds a term to this term.  Stores the result in this term.
	 * Only valid if both terms have the same alpha and beta exponents.
	 * @param term Term to be added into this term.
	 */
	protected final void add(final AlphaBetaTerm term) {
		assert (this.alphaExponent == term.alphaExponent);
		assert (this.betaExponent == term.betaExponent);
		this.coefficient += term.coefficient;
	}
	
	/**
	 * Adds two terms creating a third.
	 * Original terms are not modified.
	 * @param first Term to add.
	 * @param second Term to add.
	 * @return Result of the addition.
	 */	
	protected static AlphaBetaTerm add(final AlphaBetaTerm first, 
			final AlphaBetaTerm second) {
		assert (first.alphaExponent == second.alphaExponent);
		assert (first.betaExponent == second.betaExponent);
		return new AlphaBetaTerm(first.coefficient + second.coefficient, first.alphaExponent, first.betaExponent);
	}

	/**
	 * Subtracts one term from another creating a third.
	 * Original terms are not modified.
	 * @param first Term to subtract from.
	 * @param second Term to subtract.
	 * @return Result of the subtraction.
	 */	
	protected static AlphaBetaTerm subtract(final AlphaBetaTerm first, 
			final AlphaBetaTerm second) {
		assert (first.alphaExponent == second.alphaExponent);
		assert (first.betaExponent == second.betaExponent);
		return new AlphaBetaTerm(first.coefficient + second.coefficient, first.alphaExponent, first.betaExponent);
	}

	/**
	 * Provides a term which is the negative of this term.
	 * Such that this + newTerm = 0  
	 * @return negative term.
	 */
	protected final AlphaBetaTerm getNegative() {
		return new AlphaBetaTerm(-this.coefficient,
				this.alphaExponent, this.betaExponent);
	}
	
	/**
	 * Subtract a constant from this term.  Stores the result in this term.
	 * Only valid if this term is a constant 
	 * where both Alpha and Beta Exponent equal zero.
	 * @param constant Value to be subtracted.
	 */
	protected final void subtract(final double constant) {
		assert (alphaExponent == 0);
		assert (betaExponent == 0);
		this.coefficient -= constant;
	}
	
	/**
	 * Subtracts a term from this term.  Stores the result in this term.
	 * Only valid if both terms have the same alpha and beta exponents.
	 * @param term Term to be added into this term.
	 */
	protected final void subtract(final AlphaBetaTerm term) {
		assert (this.alphaExponent == term.alphaExponent);
		assert (this.betaExponent == term.betaExponent);
		this.coefficient -= term.coefficient;
	}

	/**
	 * Multiplies this term by a constant.  Stores the result in this term.
	 * @param constant Value that this term is to be multiplied by.
	 */
	protected final void multiplyBy(final double constant) {
		this.coefficient *= constant;
	}
	
	/**
	 * Multiplies this term by another term.  Stores the result in this term.
	 * @param term Term to multiply this term with.
	 */
	protected final void multiplyBy(final AlphaBetaTerm term) {
		this.coefficient *= term.coefficient;
		this.alphaExponent = this.alphaExponent + term.alphaExponent;
		this.betaExponent = this.betaExponent + term.betaExponent;
	}

	/**
	 * Multiplies a term and a constant creating a new term.
	 * Original term is not modified.
	 * @param first Term to multiply.
	 * @param second Constant.
	 * @return Result of the multiplication.
	 */
	protected static AlphaBetaTerm multiplyBy(final AlphaBetaTerm first, 
			final double second) {
		return new AlphaBetaTerm(first.coefficient * second, 
			first.alphaExponent,	first.betaExponent);
	} 

	/**
	 * Multiplies two terms creating a third.
	 * Original terms are not modified.
	 * @param first Term to multiply.
	 * @param second Term to multiply.
	 * @return Result of the multiplication.
	 */
	protected static AlphaBetaTerm multiplyBy(final AlphaBetaTerm first, 
			final AlphaBetaTerm second) {
		return new AlphaBetaTerm(first.coefficient * second.coefficient, 
			first.alphaExponent + second.alphaExponent,
			first.betaExponent + second.betaExponent);
	} 

	/**
	 * Divides a term by a constant.
	 * Original term is not modified.
	 * @param first Term to divide.
	 * @param second constant to divide by.
	 * @return Result of the division.
	 */
	public static AlphaBetaTerm divideBy(final AlphaBetaTerm first, 
			final double second) {
		return new AlphaBetaTerm(first.coefficient / second, 
			first.alphaExponent, first.betaExponent);
	} 

	/**
	 * Divides one term by another creating a third.
	 * Original terms are not modified.
	 * @param first Term to divide.
	 * @param second Term to divide by.
	 * @return Result of the division.
	 */
	protected static AlphaBetaTerm divideBy(final AlphaBetaTerm first, 
			final AlphaBetaTerm second) {
		return new AlphaBetaTerm(first.coefficient / second.coefficient,
			first.alphaExponent - second.alphaExponent,
			first.betaExponent - second.betaExponent);
	} 

	/**
	 * Divides this term by a constant.  Stores the result in this term.
	 * Only valid if the constant != 0;
	 * @param constant Value that this term is to be divided by.
	 */
	public final void divideBy(final double constant) {
		assert (constant != 0);
		this.coefficient /= constant;
	}
	
	/**
	 * Divide this term by another term.  Stores the result in this term.
	 * Only valid if other term != 0 
	 * @param term Term that this term is to be divided by.
	 */
	protected final void divideBy(final AlphaBetaTerm term) {
		assert (term.coefficient != 0);
		this.coefficient /= term.coefficient;
		this.alphaExponent = this.alphaExponent - term .alphaExponent;
		this.betaExponent = this.betaExponent - term.betaExponent;
	}
	
	/**
	 * Getter
	 * @return
	 */
	public double getCoefficient() {
		return this.coefficient;
	}

	/**
	 * Setter
	 * @return
	 */
	public void setCoefficient(double coeff) {
		this.coefficient = coeff;
	}
	
	/**
	 * Getter.
	 * @return The Exponent for alpha.
	 */
	protected final double getAlphaExponent() {
		return alphaExponent;
	}
	
	/**
	 * Setter.
	 * @param theAlphaExponent New exponent for alpha.
	 */
	protected final void setAlphaExponent(final double theAlphaExponent) {
		alphaExponent = theAlphaExponent;
	}
	
	/**
	 * Getter.
	 * @return The exponent for beta.
	 */
	protected final double getBetaExponent() {
		return betaExponent;
	}
	
	/**
	 * Setter.
	 * @param theBetaExponent New exponent for beta.
	 */
	protected final void setBetaExponent(final double theBetaExponent) {
		betaExponent = theBetaExponent;
	} 
	
	/**
	 * Check to see if the term is negative.
	 * @return 
	 */
	protected final boolean hasNegativeCoeffcient() {
		return (this.coefficient < 0);
	}
	
	/**
	 * Checks whether the current term is a valid monomial.
	 * As far as the CVX Matlab-based modelling system is concerned,
	 * a monomial has the form: cx_1^{a_1}...x_n^{a_n} where:
	 * c > 0 and
	 * a_i is a member of the set of real numbers.
	 *
	 * @return whether the term is a monomial or not
	 */
	public final boolean isValidMonomial() {
		return this.hasNegativeCoeffcient() && !this.isZero();
	}
	
	/**
	 * Check to see if the term equals zero.
	 * @return True If either numerator is zero 
	 * and therefore the term is negative.
	 */
	protected final boolean isZero() {
		return (this.coefficient == 0);
	}
		
	/**
	 * Checks if two terms have the same exponents.
	 * @param other Term to compare with
	 * @return True if and only if both terms 
	 * 		have the same alpha and beta exponents.
	 */
	protected final boolean hasSameExponents(final AlphaBetaTerm other) {
		if (this.alphaExponent != other.alphaExponent) {
			return false;
		}
		if (this.betaExponent != other.betaExponent) {
			return false;
		}
		return true;
	}

	/**
	 * Returns a string representation of a double, removing the ".0" after the
	 * decimal point. 
	 * @param d the double to be converted to a string
	 * @return the string representing the double
	*/
    private static String prettyPrint(final double d) {
    	final String s = new Double(d).toString();
    	if (s.endsWith(".0")) {
    	    return s.replace(".0", "");
    	} else {
    	    return s;
    	}
    }
	
	/** 
	 * Returns a string representing the term in the format used by the CVX 
	 * geometric programming solver in Matlab. 
	 * @return the string representation in CVX format.
	 */
	protected final String toCVXString() {
		StringBuffer sb = new StringBuffer();
		
		if (this.coefficient == 0) {
			return "";
		}
		
		//Coefficient (as decimal)
		if (!(this.getCoefficient() == 1 && (this.alphaExponent != 0 || this.betaExponent != 0))) {
			String coeffStr = prettyPrint(this.getCoefficient());

			sb.append(coeffStr);
			
			if (this.alphaExponent != 0 || this.betaExponent != 0) {
				sb.append("*");
			}
		}
		
		//Alpha part of the term
		if (this.alphaExponent != 0) {
			String alphaExpStr = "";
			if (this.alphaExponent != 1) {
				alphaExpStr = "^(" + prettyPrint(this.alphaExponent) + ")";
			}			
			sb.append("alpha" + alphaExpStr);
		}

		if (this.betaExponent != 0 && this.alphaExponent != 0) {
			sb.append("*");
		}
		
		//Beta part of the term
		if (this.betaExponent != 0) {
			String betaExpStr = "";
			if (this.betaExponent != 1) {
				betaExpStr = "^(" + prettyPrint(this.betaExponent) + ")";
			}			
			sb.append("beta" + betaExpStr);
		}
		
		return sb.toString();
	}
	
	/**
	 * Returns a string representing the term in the in Latex.  
	 * @param coeffAsDecimal Specifies whether coefficient should be 
	 * displayed as a fraction or decimal.
	 * @return the string representation in latex format.
	*/
	private String toLatexString(final boolean coeffAsDecimal) {
//		StringBuffer numeratorSB = new StringBuffer();
//		StringBuffer denominatorSB = new StringBuffer();
//
//		if (this.coefficient == 0) {
//			return "";  //If you want zero terms to be displayed return "0" instead of empty string here
//		}
//		
//		//Coefficient
//		
//		if (!(this.numerator == this.denominator && 
//				(this.alphaExponent != 0 || this.betaExponent != 0))) {
//			//Coefficient as decimal
//			if (coeffAsDecimal) {
//				numeratorSB.append(prettyPrint(this.numerator / this.denominator));
//			} else { //Coefficient as fraction	
//				numeratorSB.append(prettyPrint(this.numerator));
//				if (this.denominator != 1) {
//					denominatorSB.append(prettyPrint(this.denominator));
//				}
//			}		
//		}				
//		
//		//Alpha part of the term
//		if (this.alphaExponent != 0) {
//			String alphaExpStr = "";
//			if (Math.abs(this.alphaExponent) != 1) {
//				alphaExpStr = "^{" 
//					+ prettyPrint(Math.abs(this.alphaExponent)) + "}";
//			}
//			if (this.alphaExponent > 0) {
//				numeratorSB.append("\\alpha" + alphaExpStr);
//			} else {
//				denominatorSB.append("\\alpha" + alphaExpStr);
//			}
//		}
//
//		//Beta part of the term
//		if (this.betaExponent != 0) {
//			String betaExpStr = "";
//			if (Math.abs(this.betaExponent) != 1) {
//				betaExpStr = "^{" 
//					+ prettyPrint(Math.abs(this.betaExponent)) + "}";
//			}
//			if (this.betaExponent > 0) {
//				numeratorSB.append("\\beta" + betaExpStr);
//			} else {
//				denominatorSB.append("\\beta" + betaExpStr);
//			}
//		}
		
		StringBuffer sb = new StringBuffer();
//		if (denominatorSB.length() == 0) {
//			sb.append(numeratorSB);
//		} else {
//			sb.append("\\frac{");
//			sb.append(numeratorSB);
//			sb.append("}{");
//			sb.append(denominatorSB);
//			sb.append("}");	
//		}
		
		return sb.toString();		
	}
	
	/**
	 * Returns a string representing the term in the in Latex. The coefficient 
	 * may be displayed as a fraction if the numerator > 1.
	 * @return the string representation in latex format.
	 */
	public final String toLatexString() {
		return toLatexString(false);
	}
	
	/** 
	 * Returns a string representing the term in the in Latex. The coefficient 
	 * is displayed as a decimal.
	 * @return the string representation in latex format.
	 */
	protected final String toDecimalLatexString() {
		return toLatexString(true);
	}
	
	
	/**
	 * Given values for the variables, substitutes these values into the term and
	 * evaluates it.
	 * @param alpha The value for the alpha variable
	 * @param beta The value for the beta variable
	 * @return The result of evaluating the term
	 */
	protected final double evaluate(double alpha, double beta) {
		return (this.coefficient) * 
			Math.pow(alpha, this.alphaExponent) * 
			Math.pow(beta, this.betaExponent);
	}
	
	/**
	 * Testing funtion.
	 * @param args No Testing parameters.
	 * @throws IOException Never happens.
	 */
	public static void main(final String[] args) throws IOException {
		AlphaBetaTerm test = new AlphaBetaTerm(1, 1, 0);
		System.out.println(test.toLatexString());
	    double numb = 10;
	    String rptNumb;
	    DecimalFormat df = new DecimalFormat("0.000000");
	    rptNumb = df.format(numb);
	    System.out.println(numb+" "+rptNumb+"\n");
	}


	public boolean isValid() {
		double coeff = this.getCoefficient();
		System.err.println("checking term: "+this.toCVXString());
		if (Double.isNaN(coeff) || Double.isInfinite(coeff)) {
			System.err.println("bad coefficient");
			return false;
		}
		if (Double.isNaN(this.alphaExponent) || Double.isInfinite(this.alphaExponent)) {
			System.err.println("bad alpha exponent");
			return false;
		}
		if (Double.isNaN(this.betaExponent) || Double.isInfinite(this.betaExponent)) {
			System.err.println("bad beta exponent");
			return false;
		}
		return true;
	}
	
} 