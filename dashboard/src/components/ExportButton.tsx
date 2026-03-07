interface Props {
  href: string;
  label?: string;
}

export default function ExportButton({ href, label = "Export CSV" }: Props) {
  return (
    <a href={href} download className="export-btn">
      {label}
    </a>
  );
}
