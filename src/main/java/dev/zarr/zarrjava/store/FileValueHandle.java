package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.indexing.Selector;

import java.util.Optional;

public class FileValueHandle extends ValueHandle {
    Store store;
    String key;

    public FileValueHandle(Store store, String key) {
        this.store = store;
        this.key = key;
    }

    @Override
    public ValueHandle get(Selector selector) {
        if (selector.value.length != 1) throw new AssertionError();
        Optional<BufferValueHandle> valueHandle =
                store.get(key, selector.value[0]).map(BufferValueHandle::new);
        if (valueHandle.isPresent()) {
            return valueHandle.get();
        }
        return new NoneHandle();
    }

    @Override
    public void set(Selector selector, ValueHandle value) {

    }

    @Override
    public byte[] toBytes() {
        return store.get(key, null).orElse(null);
    }
}