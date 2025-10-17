import React from 'react';
import { render, waitFor, act, cleanup } from '@testing-library/react';
import { useSpotifyData, clearSpotifyDataCache } from '../useSpotifyData';

const noop = () => {};

function HookProbe({ resource = 'summary', params = {}, onChange = noop }) {
  const state = useSpotifyData(resource, params);

  React.useEffect(() => {
    onChange(state);
  }, [state, onChange, state.data, state.error, state.loading]);

  return null;
}

describe('useSpotifyData', () => {
  let originalFetch;

  beforeEach(() => {
    clearSpotifyDataCache();
    originalFetch = global.fetch;
    global.fetch = jest.fn();
    jest.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    global.fetch = originalFetch;
    jest.restoreAllMocks();
    cleanup();
  });

  it('fetches data and hits the cache on refetch', async () => {
    const mockResponse = {
      total_ms: 123_000,
      timings: { total_ms: 8.5 },
    };

    global.fetch.mockResolvedValue({
      ok: true,
      json: async () => mockResponse,
    });

    const observer = jest.fn();

    render(<HookProbe resource="summary" params={{ start: '', end: '' }} onChange={observer} />);

    await waitFor(() => {
      expect(observer).toHaveBeenCalled();
      const latest = observer.mock.calls.at(-1)[0];
      expect(latest.loading).toBe(false);
      expect(latest.error).toBeNull();
      expect(latest.data).toEqual(mockResponse);
    });

    expect(global.fetch).toHaveBeenCalledTimes(1);

    const latestState = observer.mock.calls.at(-1)[0];
    await act(async () => {
      await latestState.refetch();
    });

    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('surfaces fetch errors', async () => {
    global.fetch.mockResolvedValue({
      ok: false,
      status: 500,
      statusText: 'Server Error',
      text: async () => 'internal',
    });

    const observer = jest.fn();

    render(<HookProbe resource="summary" params={{}} onChange={observer} />);

    await waitFor(() => {
      expect(observer).toHaveBeenCalled();
      const latest = observer.mock.calls.at(-1)[0];
      expect(latest.loading).toBe(false);
      expect(latest.error).toBeInstanceOf(Error);
      expect(latest.data).toBeNull();
    });
  });
});

