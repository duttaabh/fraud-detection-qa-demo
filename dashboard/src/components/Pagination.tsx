interface Props {
  page: number;
  totalPages: number;
  onPageChange: (p: number) => void;
}

export default function Pagination({ page, totalPages, onPageChange }: Props) {
  if (totalPages <= 1) return null;
  return (
    <nav className="pagination" aria-label="Pagination">
      <button disabled={page <= 1} onClick={() => onPageChange(page - 1)}>
        ‹ Prev
      </button>

      <span className="pagination-info">
        Page
        <select
          className="pagination-select"
          value={page}
          onChange={(e) => onPageChange(Number(e.target.value))}
          aria-label="Go to page"
        >
          {Array.from({ length: totalPages }, (_, i) => (
            <option key={i + 1} value={i + 1}>
              {i + 1}
            </option>
          ))}
        </select>
        of {totalPages}
      </span>

      <button disabled={page >= totalPages} onClick={() => onPageChange(page + 1)}>
        Next ›
      </button>
    </nav>
  );
}
