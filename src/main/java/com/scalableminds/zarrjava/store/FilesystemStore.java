package com.scalableminds.zarrjava.store;

import com.scalableminds.zarrjava.indexing.OpenSlice;
import com.scalableminds.zarrjava.v3.Utils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

public class FilesystemStore implements Store, Store.ListableStore {

    @Nonnull
    private final FileSystem fileSystem;
    @Nonnull
    private final String path;

    public FilesystemStore(@Nonnull String path) {
        this.fileSystem = FileSystems.getDefault();
        this.path = path;
    }

    @Override
    public Optional<ByteBuffer> get(String key, OpenSlice byteRange) {
        Path keyPath = fileSystem.getPath(this.path, key);

        System.out.println(keyPath);
        if (byteRange == null) {
            try {
                ByteBuffer bytes = ByteBuffer.wrap(Files.readAllBytes(keyPath));
                return Optional.of(bytes);
            } catch (IOException e) {
                return Optional.empty();
            }
        }
        try (SeekableByteChannel byteChannel = Files.newByteChannel(keyPath)) {
            long startOffset = 0;
            if (byteRange.start != null) {
                if (byteRange.start >= 0) {
                    startOffset = byteRange.start;
                } else {
                    startOffset = byteChannel.size() + byteRange.start;
                }
            }
            long endOffset = byteChannel.size();
            if (byteRange.end != null) {
                if (byteRange.end >= 0) {
                    endOffset = byteRange.end;
                } else {
                    endOffset = byteChannel.size() + byteRange.end;
                }
            }

            ByteBuffer bytes = Utils.allocateNative((int) (endOffset - startOffset));
            byteChannel.position(startOffset);
            byteChannel.read(bytes);
            return Optional.of(bytes);
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public void set(String key, ByteBuffer bytes, OpenSlice byteRange) {
        Path keyPath = fileSystem.getPath(this.path, key);
        try {
            Files.createDirectories(keyPath.getParent());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (byteRange == null) {
            try (SeekableByteChannel channel = Files.newByteChannel(keyPath, StandardOpenOption.WRITE)) {
                channel.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new UnsupportedOperationException("`FilesystemStore::set` does not support range writes.");
        }
    }

    @Override
    public void delete(String key) {
        Path keyPath = fileSystem.getPath(this.path, key);
        try {
            Files.delete(keyPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterator<String> list(String key) {
        Path keyPath = fileSystem.getPath(this.path, key);
        try (Stream<Path> paths = Files.list(keyPath)) {
            return paths.map(p -> p.toFile().getName()).iterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return fileSystem.getPath(this.path).toUri().toString().replaceAll("\\/$", "");
    }

}