export interface NavItem {
  label: string;
  href: string;
}

export interface NavSection {
  title: string;
  items: NavItem[];
}

export const BASE = "/rate-limiter";

export const sidebarSections: NavSection[] = [
  {
    title: "Getting Started",
    items: [
      { label: "Introduction", href: `${BASE}/` },
      { label: "What It Is", href: `${BASE}/docs/introduction` },
      { label: "Quick Start", href: `${BASE}/docs/quickstart` },
    ],
  },
  {
    title: "Concepts",
    items: [
      { label: "Architecture", href: `${BASE}/docs/concepts/architecture` },
      { label: "Limits", href: `${BASE}/docs/concepts/limits` },
      { label: "Algorithms", href: `${BASE}/docs/concepts/algorithms` },
      { label: "Reservations", href: `${BASE}/docs/concepts/reservations` },
      { label: "Leases", href: `${BASE}/docs/concepts/leases` },
      { label: "Redis Model", href: `${BASE}/docs/concepts/redis` },
    ],
  },
  {
    title: "Installation",
    items: [
      { label: "Docker", href: `${BASE}/docs/installation/docker` },
      { label: "Configuration", href: `${BASE}/docs/installation/configuration` },
    ],
  },
  {
    title: "API Reference",
    items: [
      { label: "gRPC Service", href: `${BASE}/docs/api-reference/grpc` },
    ],
  },
  {
    title: "Operations",
    items: [
      { label: "Events", href: `${BASE}/docs/operations/events` },
      { label: "Observability", href: `${BASE}/docs/operations/observability` },
    ],
  },
  {
    title: "Deployment",
    items: [
      { label: "GitHub Actions", href: `${BASE}/docs/deployment/github-actions` },
      { label: "Kubernetes", href: `${BASE}/docs/deployment/kubernetes` },
    ],
  },
  {
    title: "Examples",
    items: [
      { label: "Product Patterns", href: `${BASE}/docs/examples/product-patterns` },
    ],
  },
  {
    title: "Project",
    items: [
      { label: "Non-Goals", href: `${BASE}/docs/project/non-goals` },
      { label: "Roadmap", href: `${BASE}/docs/project/roadmap` },
    ],
  },
];

export interface FlatNavItem extends NavItem {
  section: string;
}

export const flatNav: FlatNavItem[] = sidebarSections.flatMap((section) =>
  section.items.map((item) => ({ ...item, section: section.title })),
);

function normalize(p: string): string {
  if (!p) return p;
  if (p === BASE || p === `${BASE}/`) return `${BASE}/`;
  return p.replace(/\/+$/, "");
}

export function findCurrent(currentPath: string): FlatNavItem | undefined {
  const target = normalize(currentPath);
  return flatNav.find(
    (item) => normalize(item.href) === target || item.href === target,
  );
}

export function findPrevNext(currentPath: string): {
  prev?: FlatNavItem;
  next?: FlatNavItem;
} {
  const target = normalize(currentPath);
  const idx = flatNav.findIndex(
    (item) => normalize(item.href) === target || item.href === target,
  );
  if (idx === -1) return {};
  return {
    prev: idx > 0 ? flatNav[idx - 1] : undefined,
    next: idx < flatNav.length - 1 ? flatNav[idx + 1] : undefined,
  };
}

export function buildBreadcrumbs(
  currentPath: string,
): { label: string; href?: string }[] {
  const current = findCurrent(currentPath);
  const docsRoot = { label: "Docs", href: `${BASE}/` };
  if (!current) return [docsRoot];
  if (current.href === `${BASE}/`) return [docsRoot];
  return [docsRoot, { label: current.section }, { label: current.label }];
}
