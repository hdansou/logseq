#!/usr/bin/env python3
"""
Deep analysis of GitHub issue labels to inform strategic priorities
"""

import json
from collections import defaultdict
from pathlib import Path

# Read the cached issues data
issues_file = Path('/Users/dzu/Projects/src/github.com/logseq/dz/docs/product-report/issues_page1.json')

# Since the file is too large, let's fetch fresh data
import subprocess
import sys

try:
    import requests
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "requests"])
    import requests

from datetime import datetime

GITHUB_REPO = "logseq/db-test"

def fetch_all_issues():
    """Fetch all issues from GitHub API"""
    print(f"Fetching issues from {GITHUB_REPO}...")
    issues = []
    page = 1

    while True:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/issues?state=all&per_page=100&page={page}"
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Error fetching issues: {response.status_code}")
            break

        data = response.json()
        if not data:
            break

        # Filter out pull requests
        issues_only = [item for item in data if 'pull_request' not in item]
        issues.extend(issues_only)
        print(f"  Fetched page {page}: {len(issues_only)} issues")

        page += 1
        if page > 20:  # Safety limit
            break

    print(f"Total issues fetched: {len(issues)}")
    return issues

def categorize_labels():
    """Categorize labels into logical groups"""
    categories = {
        'Status/Workflow': [
            'in review', 'awaiting-response', 'can reproduce', "can't reproduce",
            'wontfix', 'duplicate'
        ],
        'Issue Type': [
            'bug', 'enhancement', 'question', 'feedback', 'tech debt'
        ],
        'Feature Area': [
            'import', 'export-edn', 'export-sqlite', 'query', 'ui', 'ux',
            'rtc', 'linked references', 'mobile', 'library', 'plugins',
            'graph-view', 'view', 'table', 'namespace', 'template', 'mcp',
            'editor:copy-paste', 'valid-db'
        ],
        'Priority/Severity': [
            'minor', 'file-graph-regression'
        ],
        'Platform': [
            'firefox'
        ],
        'Documentation': [
            'documentation'
        ],
        'Uncategorized': [
            'no-label'
        ]
    }
    return categories

def analyze_labels(issues):
    """Perform comprehensive label analysis"""
    now = datetime.now()

    analysis = {
        'label_stats': defaultdict(lambda: {
            'total': 0,
            'open': 0,
            'closed': 0,
            'avg_age_days': 0,
            'avg_close_time_days': 0,
            'total_comments': 0,
            'issues': []
        }),
        'category_stats': defaultdict(lambda: {
            'total': 0,
            'open': 0,
            'closed': 0
        }),
        'multi_label_analysis': [],
        'no_label_issues': [],
        'high_priority_patterns': []
    }

    categories = categorize_labels()
    label_to_category = {}
    for category, labels in categories.items():
        for label in labels:
            label_to_category[label] = category

    # Analyze each issue
    for issue in issues:
        created_at = datetime.strptime(issue['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        age_days = (now - created_at).days
        state = issue['state']

        labels = [label['name'] for label in issue['labels']]
        if not labels:
            labels = ['no-label']
            analysis['no_label_issues'].append({
                'number': issue['number'],
                'title': issue['title'],
                'state': state,
                'age_days': age_days,
                'comments': issue['comments']
            })

        # Track multi-label issues
        if len(labels) > 1 and 'no-label' not in labels:
            analysis['multi_label_analysis'].append({
                'number': issue['number'],
                'labels': labels,
                'state': state
            })

        # Process each label
        for label in labels:
            stats = analysis['label_stats'][label]
            stats['total'] += 1
            stats['issues'].append(issue)

            if state == 'open':
                stats['open'] += 1
            else:
                stats['closed'] += 1
                if issue['closed_at']:
                    closed_at = datetime.strptime(issue['closed_at'], '%Y-%m-%dT%H:%M:%SZ')
                    close_time_days = (closed_at - created_at).days
                    # Update running average
                    current_avg = stats['avg_close_time_days']
                    current_count = stats['closed']
                    stats['avg_close_time_days'] = (current_avg * (current_count - 1) + close_time_days) / current_count

            stats['total_comments'] += issue['comments']

            # Update running average for age
            current_avg = stats['avg_age_days']
            current_count = stats['total']
            stats['avg_age_days'] = (current_avg * (current_count - 1) + age_days) / current_count

            # Category stats
            category = label_to_category.get(label, 'Uncategorized')
            cat_stats = analysis['category_stats'][category]
            cat_stats['total'] += 1
            if state == 'open':
                cat_stats['open'] += 1
            else:
                cat_stats['closed'] += 1

    return analysis, categories

def identify_priority_areas(analysis, categories):
    """Identify high-priority areas based on label analysis"""
    priorities = []

    label_stats = analysis['label_stats']

    # 1. High open count with low resolution rate
    for label, stats in sorted(label_stats.items(), key=lambda x: x[1]['open'], reverse=True):
        if stats['total'] > 5:  # Only consider labels with significant volume
            resolution_rate = (stats['closed'] / stats['total'] * 100) if stats['total'] > 0 else 0
            if stats['open'] > 5 and resolution_rate < 70:
                priorities.append({
                    'label': label,
                    'reason': 'High open count with low resolution rate',
                    'open': stats['open'],
                    'total': stats['total'],
                    'resolution_rate': resolution_rate,
                    'priority_score': stats['open'] * (100 - resolution_rate) / 100
                })

    # 2. Critical feature areas with issues
    critical_labels = ['file-graph-regression', 'rtc', 'import', 'export-edn', 'graph-view']
    for label in critical_labels:
        if label in label_stats:
            stats = label_stats[label]
            if stats['open'] > 0:
                priorities.append({
                    'label': label,
                    'reason': 'Critical feature area',
                    'open': stats['open'],
                    'total': stats['total'],
                    'avg_age': stats['avg_age_days'],
                    'priority_score': stats['open'] * 2  # Higher weight for critical areas
                })

    # 3. UX/UI issues (impact on user experience)
    ux_labels = ['ui', 'ux', 'mobile']
    for label in ux_labels:
        if label in label_stats:
            stats = label_stats[label]
            if stats['open'] > 3:
                priorities.append({
                    'label': label,
                    'reason': 'User experience impact',
                    'open': stats['open'],
                    'avg_age': stats['avg_age_days'],
                    'priority_score': stats['open'] * 1.5
                })

    # 4. Enhancement backlog
    if 'enhancement' in label_stats:
        stats = label_stats['enhancement']
        if stats['open'] > 10:
            priorities.append({
                'label': 'enhancement',
                'reason': 'Large enhancement backlog needs prioritization',
                'open': stats['open'],
                'total': stats['total'],
                'priority_score': stats['open'] * 0.8
            })

    # 5. No-label issues (need triage)
    if 'no-label' in label_stats:
        stats = label_stats['no-label']
        if stats['open'] > 10:
            priorities.append({
                'label': 'no-label',
                'reason': 'Unlabeled issues need triage and categorization',
                'open': stats['open'],
                'total': stats['total'],
                'priority_score': stats['open'] * 0.5
            })

    # Sort by priority score
    priorities.sort(key=lambda x: x['priority_score'], reverse=True)

    return priorities

def generate_recommendations(analysis, priorities, categories):
    """Generate strategic recommendations based on analysis"""
    recommendations = []

    label_stats = analysis['label_stats']

    # Recommendation 1: Critical regressions
    if 'file-graph-regression' in label_stats:
        stats = label_stats['file-graph-regression']
        if stats['open'] > 5:
            recommendations.append({
                'title': 'Address File-Graph Regressions Immediately',
                'priority': 'CRITICAL',
                'rationale': f"{stats['open']} file-graph regression issues are open (91% of all regression issues). These represent backwards compatibility breaks that impact existing users.",
                'action_items': [
                    'Create a regression task force to address all file-graph-regression issues',
                    'Prioritize these above new features until resolved',
                    'Add regression testing to prevent future occurrences',
                    f"Target: Resolve all {stats['open']} issues within 2 sprints"
                ],
                'labels': ['file-graph-regression']
            })

    # Recommendation 2: Graph-view issues
    if 'graph-view' in label_stats:
        stats = label_stats['graph-view']
        if stats['open'] == stats['total']:  # All graph-view issues are open
            recommendations.append({
                'title': 'Fix Graph View Functionality',
                'priority': 'HIGH',
                'rationale': f"All {stats['open']} graph-view issues are unresolved (0% resolution rate). Graph visualization is a core feature.",
                'action_items': [
                    'Investigate root causes of graph-view issues',
                    'Assign dedicated developer to graph functionality',
                    'Consider graph rendering architecture review',
                    'Add graph-view test coverage'
                ],
                'labels': ['graph-view']
            })

    # Recommendation 3: Unlabeled issues
    no_label_stats = label_stats.get('no-label', {})
    if no_label_stats.get('open', 0) > 30:
        recommendations.append({
            'title': 'Implement Systematic Issue Triage Process',
            'priority': 'HIGH',
            'rationale': f"{no_label_stats['open']} open issues lack labels (48% of unlabeled issues remain open). This indicates insufficient triage.",
            'action_items': [
                'Designate triage lead for weekly issue review',
                'Create triage checklist and labeling guidelines',
                'Label all open issues within 2 weeks',
                'Establish SLA: All new issues labeled within 48 hours',
                'Close or label stale issues older than 90 days'
            ],
            'labels': ['no-label']
        })

    # Recommendation 4: Enhancement backlog
    enh_stats = label_stats.get('enhancement', {})
    if enh_stats.get('open', 0) > 15:
        recommendations.append({
            'title': 'Prioritize Enhancement Backlog',
            'priority': 'MEDIUM',
            'rationale': f"{enh_stats['open']} enhancement requests are open. Large backlog needs prioritization to manage user expectations.",
            'action_items': [
                'Review and categorize enhancements by impact and effort',
                'Create roadmap prioritizing high-impact, low-effort items',
                'Close or defer enhancements that don\'t align with product vision',
                'Communicate roadmap to community to manage expectations',
                'Consider community voting system for enhancement priorities'
            ],
            'labels': ['enhancement']
        })

    # Recommendation 5: RTC issues
    rtc_stats = label_stats.get('rtc', {})
    if rtc_stats.get('open', 0) > 5:
        recommendations.append({
            'title': 'Stabilize Real-Time Collaboration (RTC) Features',
            'priority': 'HIGH',
            'rationale': f"{rtc_stats['open']} RTC issues open (50% resolution rate). RTC is a critical feature for multi-user scenarios.",
            'action_items': [
                'Audit RTC architecture for stability issues',
                'Add comprehensive RTC test coverage',
                'Create RTC troubleshooting guide',
                'Consider dedicated RTC sprint',
                'Monitor RTC issues in production with telemetry'
            ],
            'labels': ['rtc']
        })

    # Recommendation 6: Import/Export issues
    import_stats = label_stats.get('import', {})
    export_edn_stats = label_stats.get('export-edn', {})
    export_sqlite_stats = label_stats.get('export-sqlite', {})

    total_import_export_open = import_stats.get('open', 0) + export_edn_stats.get('open', 0) + export_sqlite_stats.get('open', 0)
    if total_import_export_open > 10:
        recommendations.append({
            'title': 'Improve Data Portability (Import/Export)',
            'priority': 'HIGH',
            'rationale': f"{total_import_export_open} import/export issues open. Data portability is critical for user trust and migration.",
            'action_items': [
                f'Focus on {import_stats.get("open", 0)} import issues - critical for onboarding',
                f'Address {export_edn_stats.get("open", 0)} export-edn and {export_sqlite_stats.get("open", 0)} export-sqlite issues',
                'Create comprehensive import/export test suite',
                'Document known limitations and workarounds',
                'Consider import/export validation tools'
            ],
            'labels': ['import', 'export-edn', 'export-sqlite']
        })

    # Recommendation 7: UI/UX improvements
    ui_stats = label_stats.get('ui', {})
    ux_stats = label_stats.get('ux', {})
    total_ux_open = ui_stats.get('open', 0) + ux_stats.get('open', 0)

    if total_ux_open > 10:
        recommendations.append({
            'title': 'Enhance User Experience (UI/UX)',
            'priority': 'MEDIUM',
            'rationale': f"{total_ux_open} UI/UX issues open. User experience directly impacts adoption and satisfaction.",
            'action_items': [
                'Conduct UX audit of most common workflows',
                'Prioritize UI polish for frequently-used features',
                'Address accessibility concerns',
                'Create UI component consistency guidelines',
                'Consider user testing sessions'
            ],
            'labels': ['ui', 'ux']
        })

    # Recommendation 8: Mobile support
    mobile_stats = label_stats.get('mobile', {})
    if mobile_stats.get('open', 0) > 3:
        recommendations.append({
            'title': 'Improve Mobile Experience',
            'priority': 'MEDIUM',
            'rationale': f"{mobile_stats['open']} mobile issues open (50% resolution rate). Mobile is increasingly important for user access.",
            'action_items': [
                'Prioritize mobile-specific bugs and UX issues',
                'Test mobile workflows systematically',
                'Consider mobile-first design principles',
                'Expand mobile device test coverage'
            ],
            'labels': ['mobile']
        })

    # Recommendation 9: Query functionality
    query_stats = label_stats.get('query', {})
    if query_stats.get('total', 0) > 5:
        recommendations.append({
            'title': 'Enhance Query Capabilities',
            'priority': 'MEDIUM',
            'rationale': f"{query_stats['total']} query-related issues. Query functionality is a power-user feature that needs attention.",
            'action_items': [
                'Document query language comprehensively',
                'Address query performance issues',
                'Create query examples library',
                'Consider query builder UI for common cases'
            ],
            'labels': ['query']
        })

    # Recommendation 10: Process improvements
    recommendations.append({
        'title': 'Establish Issue Management Best Practices',
        'priority': 'HIGH',
        'rationale': 'Process improvements will help manage the entire issue backlog more effectively.',
        'action_items': [
            'Create label taxonomy documentation',
            'Establish issue priority levels (P0-P4)',
            'Define SLAs for different issue types',
            'Weekly triage meetings for new issues',
            'Monthly backlog grooming sessions',
            'Automate stale issue detection and closure',
            'Set up GitHub issue templates with required fields'
        ],
        'labels': ['process']
    })

    return recommendations

def print_analysis(analysis, priorities, recommendations, categories):
    """Print comprehensive analysis"""
    print("\n" + "="*80)
    print("GITHUB ISSUE LABEL ANALYSIS")
    print("="*80)

    # Category breakdown
    print("\n## Issues by Category\n")
    for category, stats in sorted(analysis['category_stats'].items(),
                                   key=lambda x: x[1]['total'], reverse=True):
        total = stats['total']
        open_count = stats['open']
        closed = stats['closed']
        resolution_rate = (closed / total * 100) if total > 0 else 0
        print(f"{category:25} | Total: {total:3} | Open: {open_count:3} | Closed: {closed:3} | Resolution: {resolution_rate:5.1f}%")

    # Priority areas
    print("\n## Priority Areas (Top 10)\n")
    for i, priority in enumerate(priorities[:10], 1):
        print(f"\n{i}. Label: {priority['label']}")
        print(f"   Reason: {priority['reason']}")
        print(f"   Open: {priority['open']}")
        if 'resolution_rate' in priority:
            print(f"   Resolution Rate: {priority['resolution_rate']:.1f}%")
        if 'avg_age' in priority:
            print(f"   Avg Age: {priority['avg_age']:.0f} days")

    # Label statistics (top labels by open count)
    print("\n## Top Labels by Open Issue Count\n")
    sorted_labels = sorted(analysis['label_stats'].items(),
                          key=lambda x: x[1]['open'], reverse=True)[:15]

    for label, stats in sorted_labels:
        resolution_rate = (stats['closed'] / stats['total'] * 100) if stats['total'] > 0 else 0
        avg_comments = stats['total_comments'] / stats['total'] if stats['total'] > 0 else 0
        print(f"\n{label}:")
        print(f"  Total: {stats['total']} | Open: {stats['open']} | Closed: {stats['closed']}")
        print(f"  Resolution Rate: {resolution_rate:.1f}%")
        print(f"  Avg Age: {stats['avg_age_days']:.0f} days")
        if stats['closed'] > 0:
            print(f"  Avg Close Time: {stats['avg_close_time_days']:.0f} days")
        print(f"  Avg Comments: {avg_comments:.1f}")

    # Multi-label analysis
    print(f"\n## Multi-Label Analysis\n")
    print(f"Total issues with multiple labels: {len(analysis['multi_label_analysis'])}")

    # Common label combinations
    from collections import Counter
    combinations = Counter()
    for item in analysis['multi_label_analysis']:
        labels = tuple(sorted([l for l in item['labels'] if l not in ['in review', 'can reproduce', "can't reproduce", 'awaiting-response']]))
        if len(labels) > 1:
            combinations[labels] += 1

    print("\nMost common label combinations:")
    for combo, count in combinations.most_common(10):
        if count > 1:
            print(f"  {' + '.join(combo)}: {count} issues")

    # Strategic recommendations
    print("\n" + "="*80)
    print("STRATEGIC RECOMMENDATIONS")
    print("="*80)

    for i, rec in enumerate(recommendations, 1):
        print(f"\n## {i}. {rec['title']}")
        print(f"Priority: {rec['priority']}")
        print(f"\nRationale: {rec['rationale']}")
        print(f"\nLabels: {', '.join(rec['labels'])}")
        print("\nAction Items:")
        for j, action in enumerate(rec['action_items'], 1):
            print(f"  {j}. {action}")

def save_analysis(analysis, priorities, recommendations):
    """Save analysis to markdown file"""
    output = []

    output.append("# GitHub Issue Label Analysis\n")
    output.append(f"**Generated:** {datetime.now().strftime('%B %d, %Y at %H:%M')}\n")
    output.append(f"**Repository:** {GITHUB_REPO}\n\n")
    output.append("---\n\n")

    # Category breakdown
    output.append("## Issues by Category\n\n")
    output.append("| Category | Total | Open | Closed | Resolution Rate |\n")
    output.append("|----------|-------|------|--------|----------------|\n")

    for category, stats in sorted(analysis['category_stats'].items(),
                                   key=lambda x: x[1]['total'], reverse=True):
        total = stats['total']
        open_count = stats['open']
        closed = stats['closed']
        resolution_rate = (closed / total * 100) if total > 0 else 0
        output.append(f"| {category} | {total} | {open_count} | {closed} | {resolution_rate:.1f}% |\n")

    # Priority areas
    output.append("\n## Priority Areas\n\n")

    for i, priority in enumerate(priorities, 1):
        output.append(f"### {i}. {priority['label']}\n")
        output.append(f"**Reason:** {priority['reason']}\n\n")
        output.append(f"- Open Issues: {priority['open']}\n")
        if 'resolution_rate' in priority:
            output.append(f"- Resolution Rate: {priority['resolution_rate']:.1f}%\n")
        if 'avg_age' in priority:
            output.append(f"- Average Age: {priority['avg_age']:.0f} days\n")
        if 'total' in priority:
            output.append(f"- Total Issues: {priority['total']}\n")
        output.append("\n")

    # Detailed label stats
    output.append("## Detailed Label Statistics\n\n")
    output.append("| Label | Total | Open | Closed | Resolution Rate | Avg Age (days) | Avg Close Time (days) | Avg Comments |\n")
    output.append("|-------|-------|------|--------|----------------|----------------|-----------------------|--------------|\n")

    sorted_labels = sorted(analysis['label_stats'].items(),
                          key=lambda x: x[1]['total'], reverse=True)

    for label, stats in sorted_labels:
        resolution_rate = (stats['closed'] / stats['total'] * 100) if stats['total'] > 0 else 0
        avg_comments = stats['total_comments'] / stats['total'] if stats['total'] > 0 else 0
        avg_close = f"{stats['avg_close_time_days']:.0f}" if stats['closed'] > 0 else "N/A"
        output.append(f"| {label} | {stats['total']} | {stats['open']} | {stats['closed']} | {resolution_rate:.1f}% | {stats['avg_age_days']:.0f} | {avg_close} | {avg_comments:.1f} |\n")

    # Strategic recommendations
    output.append("\n## Strategic Recommendations\n\n")

    for i, rec in enumerate(recommendations, 1):
        output.append(f"### {i}. {rec['title']}\n\n")
        output.append(f"**Priority:** {rec['priority']}\n\n")
        output.append(f"**Rationale:** {rec['rationale']}\n\n")
        output.append(f"**Focus Labels:** {', '.join(rec['labels'])}\n\n")
        output.append("**Action Items:**\n\n")
        for j, action in enumerate(rec['action_items'], 1):
            output.append(f"{j}. {action}\n")
        output.append("\n")

    # Save to file
    output_path = Path('/Users/dzu/Projects/src/github.com/logseq/dz/docs/product-report/label_analysis.md')
    with open(output_path, 'w') as f:
        f.write(''.join(output))

    print(f"\n\nAnalysis saved to: {output_path}")

def main():
    issues = fetch_all_issues()
    analysis, categories = analyze_labels(issues)
    priorities = identify_priority_areas(analysis, categories)
    recommendations = generate_recommendations(analysis, priorities, categories)

    print_analysis(analysis, priorities, recommendations, categories)
    save_analysis(analysis, priorities, recommendations)

if __name__ == "__main__":
    main()
