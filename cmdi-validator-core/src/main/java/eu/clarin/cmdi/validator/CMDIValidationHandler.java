/**
 * This software is copyright (c) 2014-2019 by
 *  - Institut fuer Deutsche Sprache (http://www.ids-mannheim.de)
 * This is free software. You can redistribute it
 * and/or modify it under the terms described in
 * the GNU General Public License v3 of which you
 * should have received a copy. Otherwise you can download
 * it from
 *
 *   http://www.gnu.org/licenses/gpl-3.0.txt
 *
 * @copyright Institut fuer Deutsche Sprache (http://www.ids-mannheim.de)
 *
 * @license http://www.gnu.org/licenses/gpl-3.0.txt
 *  GNU General Public License v3
 */
package eu.clarin.cmdi.validator;


public interface CMDIValidationHandler {

    public void onJobStarted() throws CMDIValidatorException;


    public void onJobFinished(CMDIValidator.Result result,
            CMDIValidator.Statistics statistics)
            throws CMDIValidatorException;


    public void onValidationReport(CMDIValidationReport report)
            throws CMDIValidatorException;

} // interface CMDIValidationHandler
