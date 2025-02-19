/*
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
package io.trino.filesystem.local;

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.Location.parse;
import static io.trino.filesystem.local.LocalUtils.handleException;

/**
 * A hierarchical file system for testing.
 */
public class LocalFileSystem
        implements TrinoFileSystem
{
    private final Path rootPath;

    public LocalFileSystem(Path rootPath)
    {
        this.rootPath = rootPath;
        checkArgument(Files.isDirectory(rootPath), "root is not a directory");
    }

    @Override
    public TrinoInputFile newInputFile(String location)
    {
        return new LocalInputFile(location, toFilePath(location));
    }

    @Override
    public TrinoInputFile newInputFile(String location, long length)
    {
        return new LocalInputFile(location, toFilePath(location), length);
    }

    @Override
    public TrinoOutputFile newOutputFile(String location)
    {
        return new LocalOutputFile(location, toFilePath(location));
    }

    @Override
    public void deleteFile(String location)
            throws IOException
    {
        Path filePath = toFilePath(location);
        try {
            Files.delete(filePath);
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public void deleteDirectory(String location)
            throws IOException
    {
        Path directoryPath = toDirectoryPath(location);
        if (!Files.exists(directoryPath)) {
            return;
        }
        if (!Files.isDirectory(directoryPath)) {
            throw new IOException("Location is not a directory: " + location);
        }

        try {
            Files.walkFileTree(
                    directoryPath,
                    new SimpleFileVisitor<>()
                    {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                                throws IOException
                        {
                            Files.delete(file);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult postVisitDirectory(Path directory, IOException exception)
                                throws IOException
                        {
                            if (exception != null) {
                                throw exception;
                            }
                            // do not delete the root of this file system
                            if (!directory.equals(rootPath)) {
                                Files.delete(directory);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    });
        }
        catch (IOException e) {
            throw handleException(location, e);
        }
    }

    @Override
    public void renameFile(String source, String target)
            throws IOException
    {
        Path sourcePath = toFilePath(source);
        Path targetPath = toFilePath(target);
        try {
            if (!Files.exists(sourcePath)) {
                throw new IOException("Source does not exist: " + source);
            }
            if (!Files.isRegularFile(sourcePath)) {
                throw new IOException("Source is not a file: " + source);
            }

            Files.createDirectories(targetPath.getParent());

            // Do not specify atomic move, as unix overwrites when atomic is enabled
            Files.move(sourcePath, targetPath);
        }
        catch (IOException e) {
            throw new IOException("File rename from %s to %s failed: %s".formatted(source, target, e.getMessage()), e);
        }
    }

    @Override
    public FileIterator listFiles(String location)
            throws IOException
    {
        return new LocalFileIterator(location, rootPath, toDirectoryPath(location));
    }

    private Path toFilePath(String fileLocation)
    {
        Location location = parseLocalLocation(fileLocation);
        location.verifyValidFileLocation();

        Path localPath = toPath(fileLocation, location);

        // local file path can not be empty as this would create a file for the root entry
        checkArgument(!localPath.equals(rootPath), "Local file location must contain a path: %s", fileLocation);
        return localPath;
    }

    private Path toDirectoryPath(String directoryLocation)
    {
        Location location = parseLocalLocation(directoryLocation);
        Path localPath = toPath(directoryLocation, location);
        return localPath;
    }

    private static Location parseLocalLocation(String locationString)
    {
        Location location = parse(locationString);
        checkArgument(location.scheme().equals(Optional.of("local")), "Only 'local' scheme is supported: %s", locationString);
        checkArgument(location.userInfo().isEmpty(), "Local location cannot contain user info: %s", locationString);
        checkArgument(location.host().isEmpty(), "Local location cannot contain a host: %s", locationString);
        return location;
    }

    private Path toPath(String locationString, Location location)
    {
        // ensure path isn't something like '../../data'
        Path localPath = rootPath.resolve(location.path()).normalize();
        checkArgument(localPath.startsWith(rootPath), "Location references data outside of the root: %s", locationString);
        return localPath;
    }
}
