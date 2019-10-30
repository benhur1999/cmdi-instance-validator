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

import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.XdmNode;


public abstract class CMDIValidatorExtension {
    protected Processor processor;


    public final void initalize(final Processor processor)
            throws CMDIValidatorInitException {
        if (processor == null) {
            throw new NullPointerException("processor == null");
        }
        this.processor = processor;
        doInitialize();
    }


    public abstract void validate(final XdmNode document,
            final CMDIValidatonReportBuilder reportBuilder)
            throws CMDIValidatorException;


    protected abstract void doInitialize() throws CMDIValidatorInitException;

} // class CMDIValidatorExtension
