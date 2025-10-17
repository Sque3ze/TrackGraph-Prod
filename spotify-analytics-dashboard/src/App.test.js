import { render, screen } from '@testing-library/react';
import App from './App';

test('renders app shell with primary controls', () => {
  render(<App />);

  // Title / header
  expect(screen.getByRole('heading', { name: /track graph/i })).toBeInTheDocument();

  // Top-level nav buttons
  expect(screen.getByRole('button', { name: /bubbles/i })).toBeInTheDocument();
  expect(screen.getByRole('button', { name: /top stats/i })).toBeInTheDocument();

  // Bubbles gate actions
  expect(screen.getByRole('button', { name: /load demo/i })).toBeInTheDocument();

  // Radiogroup exists
  expect(screen.getByRole('radiogroup', { name: 'Group bubbles by' })).toBeInTheDocument();
});
