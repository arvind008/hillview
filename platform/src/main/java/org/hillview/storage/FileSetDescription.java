/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.storage;

import org.hillview.dataset.api.IJson;
import org.hillview.table.api.ITable;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.io.Serializable;
import java.nio.file.Paths;

/**
 * Describes a set of files to load.  Not all fields are always used.
 * This class has no constructor because it is created directly by
 * (de)serialization from JSON.
 */
public class FileSetDescription implements Serializable, IJson {
    /**
     * This could probably be an enum.  Supported values are:
     * csv, orc, parquet, json, hillviewlog.
     */
    public String fileKind = "";
    /**
     * Folder where files are looked up.
     */
    public String folder = "";
    /**
     * A simple shell pattern matching file names.
     * Note: this is not a regular expression, but it can be converted
     * by the method getRegexPattern below to a regular expression.
     */
    @Nullable
    public String fileNamePattern;
    /**
     * Name of schema file in folder.  If null or empty no schema file is assumed.
     */
    @SuppressWarnings("WeakerAccess")
    @Nullable
    public String schemaFile;
    /**
     * If true the files are expected to have a header row.
     */
    public boolean headerRow = true;
    /**
     * Used to circumvent caching.
     */
    @Nullable
    public String cookie = null;
    /**
     * Used for testing: allows reading the same data multiple times.
     */
    public int repeat = 1;

    public String tags="";

    @Nullable
    private String getSchemaPath() {
        if (Utilities.isNullOrEmpty(this.schemaFile))
            return null;
        return Paths.get(this.folder, this.schemaFile).toString();
    }

    @Nullable
    public String getRegexPattern() {
        if (this.fileNamePattern == null)
            return null;
        return Utilities.wildcardToRegex(this.fileNamePattern);
    }
    
    public String getTags(){
	return this.tags;
    }

    public IFileReference createFileReference(String pathname) {
        return new FileReference(pathname);
    }

    class FileReference implements IFileReference {
        private final String pathname;

        FileReference(final String pathname) {
            this.pathname = pathname;
        }

        @Override
        public ITable load() {
            TextFileLoader loader;
            switch (FileSetDescription.this.fileKind) {
                case "csv":
                    CsvFileLoader.Config config = new CsvFileLoader.Config();
                    config.allowFewerColumns = true;
                    config.hasHeaderRow = FileSetDescription.this.headerRow;
                    loader = new CsvFileLoader(
                            this.pathname, config, FileSetDescription.this.getSchemaPath());
                    break;
                case "orc":
                    loader = new OrcFileLoader(
                            this.pathname, FileSetDescription.this.getSchemaPath(), true);
                    break;
                case "parquet":
                    loader = new ParquetFileLoader(
                            this.pathname, true);
                    break;
                case "json":
                    loader = new JsonFileLoader(
                            this.pathname, FileSetDescription.this.getSchemaPath());
                    break;
                case "hillviewlog":
                    loader = new HillviewLogs.LogFileLoader(this.pathname);
                    break;
                default:
                    throw new RuntimeException(
                            "Unexpected file kind " + FileSetDescription.this.fileKind);
            }
            return loader.load();
        }

        public long getSizeInBytes() {
            File file = new File(this.pathname);
            if (file.exists())
                return file.length();
            return 0;
        }
    }
}

    
