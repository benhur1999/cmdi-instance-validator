/**
 * This software is copyright (c) 2014 by
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

import java.io.File;

public interface CMDIWriteableValidationReport extends CMDIValidationReport {

    public void setFile(File file, boolean fileSkipped);


    public void reportInfo(int line, int col, String message);


    public void reportInfo(int line, int col, String message, Throwable cause);


    public void reportWarning(int line, int col, String message);


    public void reportWarning(int line, int col, String message,
            Throwable cause);


    public void reportError(int line, int col, String message);


    public void reportError(int line, int col, String message, Throwable cause);

} // interface CMDIWriteableValidationReport
