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
package eu.clarin.cmdi.validator.utils;

import net.sf.saxon.s9api.XdmItem;
import net.sf.saxon.s9api.XdmNode;

public final class LocationUtils {

    private LocationUtils() {
    }


    public static int getLineNumber(final XdmItem item) {
        if ((item != null) && (item instanceof XdmNode)) {
            return ((XdmNode) item).getLineNumber();
        }
        return -1;
    }


    public static int getColumnNumber(final XdmItem item) {
        if ((item != null) && (item instanceof XdmNode)) {
            return ((XdmNode) item).getColumnNumber();
        }
        return -1;
    }

} // class LocationUtils
