// jest-dom adds custom jest matchers for asserting on DOM nodes.
// Example usage:
// expect(element).toHaveTextContent(/react/i)
// learn more: https://github.com/testing-library/jest-dom
import '@testing-library/jest-dom';

// Mock all d3 APIs to no-ops so Jest doesn't try to parse ESM d3
jest.mock('d3', () => {
  // Return a proxy where any property access is a no-op function or chainable object
  const fn = () => {};
  const chain = new Proxy(fn, { get: () => chain, apply: () => chain });
  return new Proxy(
    {},
    {
      get: () => chain, // any d3.method(...) returns a chainable no-op
    }
  );
});
