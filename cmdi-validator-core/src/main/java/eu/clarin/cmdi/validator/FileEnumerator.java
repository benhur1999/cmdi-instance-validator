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
import java.util.ArrayDeque;
import java.util.List;

import net.java.truevfs.access.TFile;

final class FileEnumerator {
    private static final int INITIAL_QUEUE_SIZE = 1024;
    private final ArrayDeque<TFile> queue =
            new ArrayDeque<>(INITIAL_QUEUE_SIZE);
    private final FileFilter filter;


    FileEnumerator(List<TFile> fileList, FileFilter filter) {
        if (fileList == null) {
            throw new NullPointerException("files == null");
        }
        if (fileList.isEmpty()) {
            throw new IllegalArgumentException("files is empty");
        }

        if (filter != null) {
            this.filter = filter;
        } else {
            this.filter = new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return true;
                }
            };
        }
        queue.addAll(fileList);
    }


    public boolean isEmpty() {
        return queue.isEmpty();
    }


    public synchronized TFile nextFile() {
        TFile result = null;
        while (!queue.isEmpty()) {
            TFile entry = queue.removeFirst();
            if (entry.isDirectory()) {
                addEntries(entry);
            } else {
                result = entry;
                break;
            }
        } // for
        return result;
    }
    

    private void addEntries(TFile entry) {
        if (entry.isFile() && filter.accept(entry)) {
            queue.addFirst(entry);
        } else if (entry.isDirectory()) {
            if (entry.isArchive() && !filter.accept(entry)) {
                return;
            }
            TFile[] entries = entry.listFiles();
            if (entries.length > 0) {
                for (int i = 0; i < entries.length; i++) {
                    final TFile tmp = entries[i];
                    if (tmp.isFile()) {
                        if (!filter.accept(tmp)) {
                            continue;
                        }
                    }
                    queue.addFirst(tmp);
                }
            }
        }
    }

} // class FileEnumerator
