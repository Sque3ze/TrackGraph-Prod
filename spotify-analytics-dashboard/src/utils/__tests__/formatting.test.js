import { formatHours, formatPercent, formatNumber, truncateText, hashColor } from '../formatting';

describe('formatting utilities', () => {
  describe('formatHours', () => {
    it('formats numbers with two decimal places and suffix', () => {
      expect(formatHours(1)).toBe('1.00h');
      expect(formatHours(1.236)).toBe('1.24h');
    });

    it('handles nullish values as zero', () => {
      expect(formatHours(null)).toBe('0.00h');
      expect(formatHours(undefined)).toBe('0.00h');
    });
  });

  describe('formatPercent', () => {
    it('formats fractional values as percentages', () => {
      expect(formatPercent(0.42)).toBe('42.00%');
      expect(formatPercent(0.1234)).toBe('12.34%');
    });

    it('returns 0.00% when value is not finite', () => {
      expect(formatPercent(null)).toBe('0.00%');
      expect(formatPercent(undefined)).toBe('0.00%');
      expect(formatPercent(NaN)).toBe('0.00%');
    });
  });

  describe('formatNumber', () => {
    it('adds thousands separators', () => {
      expect(formatNumber(1234567)).toMatch(/1,234,567|1 234 567/);
    });

    it('handles missing values', () => {
      expect(formatNumber()).toBe('0');
    });
  });

  describe('truncateText', () => {
    it('returns original string when below limit', () => {
      expect(truncateText('Hello', 10)).toBe('Hello');
    });

    it('adds ellipsis when above limit', () => {
      expect(truncateText('Playwright', 5)).toBe('Play…');
    });
  });

  describe('hashColor', () => {
    it('returns deterministic hsl string', () => {
      const first = hashColor('artist-123');
      const second = hashColor('artist-123');
      expect(first).toMatch(/^hsl\(\d+,70%,45%\)$/);
      expect(second).toBe(first);
    });

    it('varies with input id', () => {
      expect(hashColor('artist-a')).not.toBe(hashColor('artist-b'));
    });
  });
});

