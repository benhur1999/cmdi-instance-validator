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

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class CMDIValidatorConfig {
    private final File root;
    private final CMDIValidationHandler handler;
    private FileFilter fileFilter = null;
    private long maxFileSize = 0;
    private File schemaCacheDirectory = null;
    private CMDISchemaLoader schemaLoader = null;
    private File schematronSchemaFile = null;
    private boolean schematronDisabled = false;
    private List<CMDIValidatorExtension> extensions = null;
    private int connectTimeout = 60000;
    private int socketTimeout = 60000;


    private CMDIValidatorConfig(final File root,
            final CMDIValidationHandler handler) {
        if (root == null) {
            throw new NullPointerException("root = null");
        }
        if (handler == null) {
            throw new NullPointerException("handler == null");
        }
        this.root    = root;
        this.handler = handler;
    }


    public File getRoot() {
        return root;
    }


    public FileFilter getFileFilter() {
        return fileFilter;
    }


    public long getMaxFileSize() {
        return maxFileSize;
    }


    public CMDIValidationHandler getHandler() {
        return handler;
    }


    public File getSchemaCacheDirectory() {
        return schemaCacheDirectory;
    }


    public CMDISchemaLoader getSchemaLoader() {
        return schemaLoader;
    }


    public File getSchematronSchemaFile() {
        return schematronSchemaFile;
    }


    public boolean isSchematronDisabled() {
        return schematronDisabled;
    }


    public List<CMDIValidatorExtension> getExtensions() {
        if (extensions != null) {
            return Collections.unmodifiableList(extensions);
        } else {
            return null;
        }
    }


    public int getConnectTimeout() {
        return connectTimeout;
    }


    public int getSocketTimeout() {
        return socketTimeout;
    }


    public static class Builder {
        private final CMDIValidatorConfig config;


        public Builder(final File root, final CMDIValidationHandler handler) {
            if (root == null) {
                throw new NullPointerException("root == null");
            }
            if (handler == null) {
                throw new NullPointerException("handler == null");
            }
            this.config = new CMDIValidatorConfig(root, handler);
        }


        public Builder fileFilter(final FileFilter fileFilter) {
            config.fileFilter = fileFilter;
            return this;
        }


        public Builder maxFileSize(final long maxFileSize) {
            if (maxFileSize < 0) {
                throw new IllegalArgumentException("maxFileSize needs to be larger or equal to zero");
            }
            config.maxFileSize = maxFileSize;
            return this;
        }


        public Builder schemaCacheDirectory(final File schemaCacheDirectory) {
            if (schemaCacheDirectory == null) {
                throw new NullPointerException("schemaCacheDirectory == null");
            }
            if (!schemaCacheDirectory.isDirectory()) {
                throw new IllegalArgumentException("'" + schemaCacheDirectory +
                        "'is not a directory");
            }
            config.schemaCacheDirectory = schemaCacheDirectory;
            return this;
        }


        public Builder schemaLoader(final CMDISchemaLoader schemaLoader) {
            if (schemaLoader == null) {
                throw new NullPointerException("schemaLoader == null");
            }
            config.schemaLoader = schemaLoader;
            return this;
        }


        public Builder schematronSchemaFile(final File schematronSchemaFile) {
            if (schematronSchemaFile == null) {
                throw new NullPointerException("schematronSchemaFile == null");
            }
            if (!schematronSchemaFile.isFile()) {
                throw new IllegalArgumentException("'" + schematronSchemaFile +
                        "'is not a regular file");
            }
            config.schematronSchemaFile = schematronSchemaFile;
            return this;
        }


        public Builder schematronDisabled(boolean schematronDisabled) {
            config.schematronDisabled = schematronDisabled;
            return this;
        }


        public Builder disableSchematron() {
            config.schematronDisabled = true;
            return this;
        }


        public Builder extension(final CMDIValidatorExtension extension) {
            if (extension == null) {
                throw new NullPointerException("extension == null");
            }
            if (config.extensions == null) {
                config.extensions = new ArrayList<CMDIValidatorExtension>();
            }
            config.extensions.add(extension);
            return this;
        }


        public Builder connectTimeout(int connectTimeout) {
            config.connectTimeout = connectTimeout;
            return this;
        }


        public Builder socketTimeout(int socketTimeout) {
            config.socketTimeout = socketTimeout;
            return this;
        }


        public CMDIValidatorConfig build() {
            return config;
        }

    } // class Builder

} // class CMDIValidatorFactoryConfig
