#!/usr/bin/env python3
"""
Executive Report Generator for Logseq Project
Analyzes GitHub issues and git repository data to create comprehensive reports
"""

import json
import subprocess
import sys
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from pathlib import Path
import re

# Check and install required packages
try:
    import requests
    from pptx import Presentation
    from pptx.util import Inches, Pt
    from pptx.enum.text import PP_ALIGN
    from pptx.dml.color import RGBColor
    from pptx.chart.data import CategoryChartData
    from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import numpy as np
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Installing required packages...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "requests", "python-pptx", "matplotlib", "numpy"])
    import requests
    from pptx import Presentation
    from pptx.util import Inches, Pt
    from pptx.enum.text import PP_ALIGN
    from pptx.dml.color import RGBColor
    from pptx.chart.data import CategoryChartData
    from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import numpy as np

# Configuration
GITHUB_REPO = "logseq/db-test"
GIT_REPO_PATH = "/Users/dzu/Projects/src/github.com/logseq"
OUTPUT_DIR = Path("/Users/dzu/Projects/src/github.com/logseq/dz/docs/product-report")
BRANCH = "master"
MONTHS_TO_ANALYZE = 24

# Color scheme - Professional and bold
COLORS = {
    'primary': RGBColor(44, 62, 80),      # Dark blue-gray
    'accent': RGBColor(231, 76, 60),      # Bold red
    'secondary': RGBColor(52, 152, 219),  # Bright blue
    'success': RGBColor(46, 204, 113),    # Green
    'warning': RGBColor(241, 196, 15),    # Yellow
    'neutral': RGBColor(236, 240, 241),   # Light gray
    'dark': RGBColor(52, 73, 94)          # Darker gray
}

class GitHubAnalyzer:
    """Analyze GitHub issues data"""

    def __init__(self, repo):
        self.repo = repo
        self.issues = []

    def fetch_all_issues(self):
        """Fetch all issues from GitHub API"""
        print(f"Fetching issues from {self.repo}...")
        page = 1
        while True:
            url = f"https://api.github.com/repos/{self.repo}/issues?state=all&per_page=100&page={page}"
            response = requests.get(url)

            if response.status_code != 200:
                print(f"Error fetching issues: {response.status_code}")
                break

            data = response.json()
            if not data:
                break

            # Filter out pull requests
            issues_only = [item for item in data if 'pull_request' not in item]
            self.issues.extend(issues_only)
            print(f"  Fetched page {page}: {len(issues_only)} issues")

            page += 1

            # Safety limit
            if page > 20:
                break

        print(f"Total issues fetched: {len(self.issues)}")
        return self.issues

    def analyze_issues(self):
        """Analyze issues and compute metrics"""
        now = datetime.now()

        analysis = {
            'total_issues': len(self.issues),
            'open_issues': 0,
            'closed_issues': 0,
            'by_label': defaultdict(list),
            'by_state_and_label': defaultdict(lambda: {'open': 0, 'closed': 0}),
            'reporters': Counter(),
            'close_times': [],
            'issue_ages': [],
            'old_open_issues': [],
            'engagement_scores': []
        }

        for issue in self.issues:
            created_at = datetime.strptime(issue['created_at'], '%Y-%m-%dT%H:%M:%SZ')
            state = issue['state']

            # Basic counts
            if state == 'open':
                analysis['open_issues'] += 1
            else:
                analysis['closed_issues'] += 1

            # Labels
            labels = [label['name'] for label in issue['labels']]
            if not labels:
                labels = ['no-label']

            for label in labels:
                analysis['by_label'][label].append(issue)
                if state == 'open':
                    analysis['by_state_and_label'][label]['open'] += 1
                else:
                    analysis['by_state_and_label'][label]['closed'] += 1

            # Reporter
            if issue['user']:
                analysis['reporters'][issue['user']['login']] += 1

            # Close time (for closed issues)
            if state == 'closed' and issue['closed_at']:
                closed_at = datetime.strptime(issue['closed_at'], '%Y-%m-%dT%H:%M:%SZ')
                days_to_close = (closed_at - created_at).days
                analysis['close_times'].append(days_to_close)

            # Issue age
            age_days = (now - created_at).days
            analysis['issue_ages'].append(age_days)

            # Track old open issues for problem areas
            if state == 'open':
                engagement = issue['comments']
                analysis['old_open_issues'].append({
                    'number': issue['number'],
                    'title': issue['title'],
                    'age_days': age_days,
                    'comments': engagement,
                    'labels': labels,
                    'url': issue['html_url'],
                    'score': age_days * 0.5 + engagement * 10  # Combined score
                })
                analysis['engagement_scores'].append(engagement)

        # Calculate metrics
        if analysis['close_times']:
            analysis['avg_close_time'] = sum(analysis['close_times']) / len(analysis['close_times'])
            analysis['median_close_time'] = sorted(analysis['close_times'])[len(analysis['close_times']) // 2]
        else:
            analysis['avg_close_time'] = 0
            analysis['median_close_time'] = 0

        if analysis['engagement_scores']:
            analysis['avg_engagement'] = sum(analysis['engagement_scores']) / len(analysis['engagement_scores'])
        else:
            analysis['avg_engagement'] = 0

        # Resolution rate
        if analysis['total_issues'] > 0:
            analysis['resolution_rate'] = (analysis['closed_issues'] / analysis['total_issues']) * 100
        else:
            analysis['resolution_rate'] = 0

        # Sort problem areas by score
        analysis['old_open_issues'].sort(key=lambda x: x['score'], reverse=True)

        return analysis


class GitAnalyzer:
    """Analyze git repository data"""

    def __init__(self, repo_path, branch='master'):
        self.repo_path = Path(repo_path)
        self.branch = branch

    def get_commit_history(self, months=24):
        """Get commit history for the last N months"""
        print(f"\nAnalyzing git history for last {months} months...")

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=months * 30)

        # Get commits
        cmd = [
            'git', '-C', str(self.repo_path),
            'log', self.branch,
            '--since', start_date.strftime('%Y-%m-%d'),
            '--format=%H|%an|%ae|%at|%s',
            '--no-merges'
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        commits = []
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            parts = line.split('|', 4)
            if len(parts) == 5:
                commits.append({
                    'hash': parts[0],
                    'author': parts[1],
                    'email': parts[2],
                    'timestamp': int(parts[3]),
                    'message': parts[4]
                })

        print(f"Found {len(commits)} commits")
        return commits

    def analyze_commits(self, commits):
        """Analyze commit data"""
        analysis = {
            'total_commits': len(commits),
            'by_month': defaultdict(int),
            'by_author': defaultdict(int),
            'contributors': set(),
            'monthly_contributors': defaultdict(set),
            'commit_messages': []
        }

        for commit in commits:
            # Parse date
            dt = datetime.fromtimestamp(commit['timestamp'])
            month_key = dt.strftime('%Y-%m')

            # Count by month
            analysis['by_month'][month_key] += 1

            # Count by author
            author = commit['author']
            analysis['by_author'][author] += 1
            analysis['contributors'].add(author)
            analysis['monthly_contributors'][month_key].add(author)

            # Store message for milestone detection
            analysis['commit_messages'].append(commit['message'])

        # Calculate monthly active contributors
        analysis['avg_monthly_contributors'] = sum(
            len(contributors) for contributors in analysis['monthly_contributors'].values()
        ) / max(len(analysis['monthly_contributors']), 1)

        return analysis

    def get_contributor_stats(self):
        """Get detailed contributor statistics"""
        print("\nAnalyzing contributor statistics...")

        cmd = [
            'git', '-C', str(self.repo_path),
            'shortlog', '-sn', '--no-merges', self.branch
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)

        contributors = []
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            match = re.match(r'\s*(\d+)\s+(.+)', line)
            if match:
                contributors.append({
                    'commits': int(match.group(1)),
                    'name': match.group(2)
                })

        return contributors


class ReportGenerator:
    """Generate PowerPoint and Markdown reports"""

    def __init__(self, github_analysis, git_analysis, output_dir):
        self.github = github_analysis
        self.git = git_analysis
        self.output_dir = Path(output_dir)
        self.charts_dir = self.output_dir / 'charts'
        self.charts_dir.mkdir(exist_ok=True)

    def generate_chart(self, data, chart_type, title, filename, **kwargs):
        """Generate a chart using matplotlib"""
        plt.figure(figsize=(10, 6))

        if chart_type == 'bar':
            plt.bar(data['x'], data['y'], color='#3498DB')
            plt.xlabel(data.get('xlabel', ''))
            plt.ylabel(data.get('ylabel', ''))
            if kwargs.get('rotate_labels'):
                plt.xticks(rotation=45, ha='right')

        elif chart_type == 'horizontal_bar':
            plt.barh(data['y'], data['x'], color='#3498DB')
            plt.xlabel(data.get('xlabel', ''))
            plt.ylabel(data.get('ylabel', ''))

        elif chart_type == 'pie':
            colors = ['#3498DB', '#E74C3C', '#2ECC71', '#F1C40F', '#9B59B6']
            plt.pie(data['values'], labels=data['labels'], autopct='%1.1f%%', colors=colors)

        elif chart_type == 'line':
            plt.plot(data['x'], data['y'], marker='o', linewidth=2, color='#3498DB')
            plt.xlabel(data.get('xlabel', ''))
            plt.ylabel(data.get('ylabel', ''))
            plt.grid(True, alpha=0.3)
            if kwargs.get('rotate_labels'):
                plt.xticks(rotation=45, ha='right')

        plt.title(title, fontsize=14, fontweight='bold')
        plt.tight_layout()

        filepath = self.charts_dir / filename
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close()

        return filepath

    def create_powerpoint(self):
        """Create PowerPoint presentation"""
        print("\nGenerating PowerPoint presentation...")

        prs = Presentation()
        prs.slide_width = Inches(10)
        prs.slide_height = Inches(7.5)

        # Slide 1: Title
        self._add_title_slide(prs)

        # Slide 2: Executive Dashboard
        self._add_dashboard_slide(prs)

        # Slide 3: All Issues by Label
        self._add_all_issues_by_label_slide(prs)

        # Slide 4: Open Issues by Label
        self._add_open_issues_by_label_slide(prs)

        # Slide 5: Issue Status Pie Chart
        self._add_issue_status_slide(prs)

        # Slide 6: Top Issue Reporters
        self._add_top_reporters_slide(prs)

        # Slide 7: Problem Areas
        self._add_problem_areas_slide(prs)

        # Slide 8: Commit History Trend
        self._add_commit_trend_slide(prs)

        # Slide 9: Top Contributors
        self._add_top_contributors_slide(prs)

        # Slide 10: Project Health
        self._add_project_health_slide(prs)

        # Slide 11: Recommendations
        self._add_recommendations_slide(prs)

        # Save
        output_path = self.output_dir / 'logseq_executive_report.pptx'
        prs.save(output_path)
        print(f"PowerPoint saved to: {output_path}")

        return output_path

    def _add_title_slide(self, prs):
        """Add title slide"""
        slide = prs.slides.add_slide(prs.slide_layouts[6])  # Blank layout

        # Title
        title_box = slide.shapes.add_textbox(Inches(1), Inches(2.5), Inches(8), Inches(1))
        title_frame = title_box.text_frame
        title_frame.text = "Logseq Project Executive Report"
        title_para = title_frame.paragraphs[0]
        title_para.alignment = PP_ALIGN.CENTER
        title_para.font.size = Pt(44)
        title_para.font.bold = True
        title_para.font.color.rgb = COLORS['primary']

        # Subtitle
        subtitle_box = slide.shapes.add_textbox(Inches(1), Inches(3.7), Inches(8), Inches(0.8))
        subtitle_frame = subtitle_box.text_frame
        subtitle_frame.text = f"Repository: {GITHUB_REPO}"
        subtitle_para = subtitle_frame.paragraphs[0]
        subtitle_para.alignment = PP_ALIGN.CENTER
        subtitle_para.font.size = Pt(24)
        subtitle_para.font.color.rgb = COLORS['secondary']

        # Date
        date_box = slide.shapes.add_textbox(Inches(1), Inches(4.7), Inches(8), Inches(0.5))
        date_frame = date_box.text_frame
        date_frame.text = f"Generated: {datetime.now().strftime('%B %d, %Y')}"
        date_para = date_frame.paragraphs[0]
        date_para.alignment = PP_ALIGN.CENTER
        date_para.font.size = Pt(16)
        date_para.font.color.rgb = COLORS['dark']

    def _add_dashboard_slide(self, prs):
        """Add executive dashboard slide"""
        slide = prs.slides.add_slide(prs.slide_layouts[6])

        # Title
        title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(9), Inches(0.6))
        title_frame = title_box.text_frame
        title_frame.text = "Executive Dashboard"
        title_para = title_frame.paragraphs[0]
        title_para.font.size = Pt(32)
        title_para.font.bold = True
        title_para.font.color.rgb = COLORS['primary']

        # Metrics boxes
        metrics = [
            ("Total Issues", self.github['total_issues'], COLORS['primary']),
            ("Open Issues", self.github['open_issues'], COLORS['accent']),
            ("Closed Issues", self.github['closed_issues'], COLORS['success']),
            ("Resolution Rate", f"{self.github['resolution_rate']:.1f}%", COLORS['secondary']),
            ("Avg Close Time", f"{self.github['median_close_time']:.0f} days", COLORS['warning']),
            ("Contributors", len(self.git['contributors']), COLORS['dark'])
        ]

        x_positions = [0.5, 3.5, 6.5, 0.5, 3.5, 6.5]
        y_positions = [1.5, 1.5, 1.5, 4, 4, 4]

        for i, (label, value, color) in enumerate(metrics):
            # Box background
            box = slide.shapes.add_shape(
                1,  # Rectangle
                Inches(x_positions[i]), Inches(y_positions[i]),
                Inches(2.5), Inches(1.8)
            )
            box.fill.solid()
            box.fill.fore_color.rgb = color
            box.line.color.rgb = color

            # Value
            value_box = slide.shapes.add_textbox(
                Inches(x_positions[i]), Inches(y_positions[i] + 0.3),
                Inches(2.5), Inches(0.8)
            )
            value_frame = value_box.text_frame
            value_frame.text = str(value)
            value_para = value_frame.paragraphs[0]
            value_para.alignment = PP_ALIGN.CENTER
            value_para.font.size = Pt(36)
            value_para.font.bold = True
            value_para.font.color.rgb = RGBColor(255, 255, 255)

            # Label
            label_box = slide.shapes.add_textbox(
                Inches(x_positions[i]), Inches(y_positions[i] + 1.1),
                Inches(2.5), Inches(0.5)
            )
            label_frame = label_box.text_frame
            label_frame.text = label
            label_para = label_frame.paragraphs[0]
            label_para.alignment = PP_ALIGN.CENTER
            label_para.font.size = Pt(14)
            label_para.font.color.rgb = RGBColor(255, 255, 255)

    def _add_all_issues_by_label_slide(self, prs):
        """Add all issues by label chart"""
        # Generate chart
        labels = sorted(self.github['by_label'].keys(), key=lambda x: len(self.github['by_label'][x]), reverse=True)[:15]
        counts = [len(self.github['by_label'][label]) for label in labels]

        chart_path = self.generate_chart(
            {'x': labels, 'y': counts, 'xlabel': 'Label', 'ylabel': 'Number of Issues'},
            'bar',
            'All Issues by Label (Top 15)',
            'all_issues_by_label.png',
            rotate_labels=True
        )

        slide = prs.slides.add_slide(prs.slide_layouts[6])

        # Title
        title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(9), Inches(0.6))
        title_frame = title_box.text_frame
        title_frame.text = "All Issues by Label"
        title_para = title_frame.paragraphs[0]
        title_para.font.size = Pt(32)
        title_para.font.bold = True
        title_para.font.color.rgb = COLORS['primary']

        # Add chart image
        slide.shapes.add_picture(str(chart_path), Inches(0.5), Inches(1.2), width=Inches(9))

    def _add_open_issues_by_label_slide(self, prs):
        """Add open issues by label chart"""
        # Generate chart
        open_by_label = {k: v['open'] for k, v in self.github['by_state_and_label'].items() if v['open'] > 0}
        labels = sorted(open_by_label.keys(), key=lambda x: open_by_label[x], reverse=True)[:15]
        counts = [open_by_label[label] for label in labels]

        chart_path = self.generate_chart(
            {'x': labels, 'y': counts, 'xlabel': 'Label', 'ylabel': 'Number of Open Issues'},
            'bar',
            'Currently Open Issues by Label (Top 15)',
            'open_issues_by_label.png',
            rotate_labels=True
        )

        slide = prs.slides.add_slide(prs.slide_layouts[6])

        # Title
        title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(9), Inches(0.6))
        title_frame = title_box.text_frame
        title_frame.text = "Currently Open Issues by Label"
        title_para = title_frame.paragraphs[0]
        title_para.font.size = Pt(32)
        title_para.font.bold = True
        title_para.font.color.rgb = COLORS['primary']

        # Add chart image
        slide.shapes.add_picture(str(chart_path), Inches(0.5), Inches(1.2), width=Inches(9))

    def _add_issue_status_slide(self, prs):
        """Add issue status pie chart"""
        # Generate chart
        chart_path = self.generate_chart(
            {
                'values': [self.github['closed_issues'], self.github['open_issues']],
                'labels': [
                    f"Closed ({self.github['closed_issues']})",
                    f"Open ({self.github['open_issues']})"
                ]
            },
            'pie',
            'Issue Status Distribution',
            'issue_status.png'
        )

        slide = prs.slides.add_slide(prs.slide_layouts[6])

        # Title
        title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(9), Inches(0.6))
        title_frame = title_box.text_frame
        title_frame.text = "Issue Status Summary"
        title_para = title_frame.paragraphs[0]
        title_para.font.size = Pt(32)
        title_para.font.bold = True
        title_para.font.color.rgb = COLORS['primary']

        # Add chart image
        slide.shapes.add_picture(str(chart_path), Inches(1.5), Inches(1.5), width=Inches(7))

        # Add resolution metrics
        metrics_box = slide.shapes.add_textbox(Inches(1), Inches(5.5), Inches(8), Inches(1.5))
        metrics_frame = metrics_box.text_frame
        metrics_text = f"Resolution Rate: {self.github['resolution_rate']:.1f}% | "
        metrics_text += f"Median Time to Close: {self.github['median_close_time']:.0f} days"
        metrics_frame.text = metrics_text
        metrics_para = metrics_frame.paragraphs[0]
        metrics_para.alignment = PP_ALIGN.CENTER
        metrics_para.font.size = Pt(18)
        metrics_para.font.bold = True
        metrics_para.font.color.rgb = COLORS['secondary']

    def _add_top_reporters_slide(self, prs):
        """Add top issue reporters chart"""
        # Get top 10 reporters
        top_reporters = self.github['reporters'].most_common(10)
        names = [name for name, _ in top_reporters]
        counts = [count for _, count in top_reporters]

        chart_path = self.generate_chart(
            {'y': names, 'x': counts, 'xlabel': 'Number of Issues Reported', 'ylabel': 'Reporter'},
            'horizontal_bar',
            'Top 10 Issue Reporters',
            'top_reporters.png'
        )

        slide = prs.slides.add_slide(prs.slide_layouts[6])

        # Title
        title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(9), Inches(0.6))
        title_frame = title_box.text_frame
        title_frame.text = "Top Issue Reporters"
        title_para = title_frame.paragraphs[0]
        title_para.font.size = Pt(32)
        title_para.font.bold = True
        title_para.font.color.rgb = COLORS['primary']

        # Add chart image
        slide.shapes.add_picture(str(chart_path), Inches(0.5), Inches(1.2), width=Inches(9))

    def _add_problem_areas_slide(self, prs):
        """Add top problem areas slide"""
        slide = prs.slides.add_slide(prs.slide_layouts[6])

        # Title
        title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(9), Inches(0.6))
        title_frame = title_box.text_frame
        title_frame.text = "Top Problem Areas"
        title_para = title_frame.paragraphs[0]
        title_para.font.size = Pt(32)
        title_para.font.bold = True
        title_para.font.color.rgb = COLORS['primary']

        # Add top 4 problem issues
        top_issues = self.github['old_open_issues'][:4]

        y_pos = 1.2
        for i, issue in enumerate(top_issues):
            # Issue box
            box = slide.shapes.add_shape(
                1,  # Rectangle
                Inches(0.5), Inches(y_pos),
                Inches(9), Inches(1.3)
            )
            box.fill.solid()
            box.fill.fore_color.rgb = COLORS['neutral']
            box.line.color.rgb = COLORS['primary']

            # Issue title
            title_text = f"#{issue['number']}: {issue['title'][:80]}"
            if len(issue['title']) > 80:
                title_text += "..."

            title_box = slide.shapes.add_textbox(
                Inches(0.7), Inches(y_pos + 0.1),
                Inches(8.6), Inches(0.5)
            )
            title_frame = title_box.text_frame
            title_frame.text = title_text
            title_para = title_frame.paragraphs[0]
            title_para.font.size = Pt(14)
            title_para.font.bold = True
            title_para.font.color.rgb = COLORS['primary']

            # Issue metadata
            meta_text = f"Age: {issue['age_days']} days | Comments: {issue['comments']} | Labels: {', '.join(issue['labels'][:3])}"

            meta_box = slide.shapes.add_textbox(
                Inches(0.7), Inches(y_pos + 0.6),
                Inches(8.6), Inches(0.4)
            )
            meta_frame = meta_box.text_frame
            meta_frame.text = meta_text
            meta_para = meta_frame.paragraphs[0]
            meta_para.font.size = Pt(11)
            meta_para.font.color.rgb = COLORS['dark']

            y_pos += 1.5

    def _add_commit_trend_slide(self, prs):
        """Add commit history trend chart"""
        # Prepare monthly data
        months = sorted(self.git['by_month'].keys())
        counts = [self.git['by_month'][m] for m in months]

        chart_path = self.generate_chart(
            {'x': months, 'y': counts, 'xlabel': 'Month', 'ylabel': 'Number of Commits'},
            'line',
            f'Commit History Trend (Last {MONTHS_TO_ANALYZE} Months)',
            'commit_trend.png',
            rotate_labels=True
        )

        slide = prs.slides.add_slide(prs.slide_layouts[6])

        # Title
        title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(9), Inches(0.6))
        title_frame = title_box.text_frame
        title_frame.text = "Commit Activity Trend"
        title_para = title_frame.paragraphs[0]
        title_para.font.size = Pt(32)
        title_para.font.bold = True
        title_para.font.color.rgb = COLORS['primary']

        # Add chart image
        slide.shapes.add_picture(str(chart_path), Inches(0.5), Inches(1.2), width=Inches(9))

    def _add_top_contributors_slide(self, prs):
        """Add top contributors chart"""
        # Get top 10 contributors
        top_contributors = sorted(self.git['by_author'].items(), key=lambda x: x[1], reverse=True)[:10]
        names = [name for name, _ in top_contributors]
        commits = [count for _, count in top_contributors]

        chart_path = self.generate_chart(
            {'y': names, 'x': commits, 'xlabel': 'Number of Commits', 'ylabel': 'Contributor'},
            'horizontal_bar',
            'Top 10 Contributors (by commits)',
            'top_contributors.png'
        )

        slide = prs.slides.add_slide(prs.slide_layouts[6])

        # Title
        title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(9), Inches(0.6))
        title_frame = title_box.text_frame
        title_frame.text = "Top Code Contributors"
        title_para = title_frame.paragraphs[0]
        title_para.font.size = Pt(32)
        title_para.font.bold = True
        title_para.font.color.rgb = COLORS['primary']

        # Add chart image
        slide.shapes.add_picture(str(chart_path), Inches(0.5), Inches(1.2), width=Inches(9))

    def _add_project_health_slide(self, prs):
        """Add project health assessment slide"""
        slide = prs.slides.add_slide(prs.slide_layouts[6])

        # Title
        title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(9), Inches(0.6))
        title_frame = title_box.text_frame
        title_frame.text = "Project Health Assessment"
        title_para = title_frame.paragraphs[0]
        title_para.font.size = Pt(32)
        title_para.font.bold = True
        title_para.font.color.rgb = COLORS['primary']

        # Health metrics
        avg_commits_per_month = self.git['total_commits'] / MONTHS_TO_ANALYZE if MONTHS_TO_ANALYZE > 0 else 0

        metrics = [
            ("Issue Resolution Rate", f"{self.github['resolution_rate']:.1f}%",
             "High" if self.github['resolution_rate'] > 70 else "Medium" if self.github['resolution_rate'] > 50 else "Low"),
            ("Monthly Commit Velocity", f"{avg_commits_per_month:.0f} commits/month",
             "High" if avg_commits_per_month > 100 else "Medium" if avg_commits_per_month > 50 else "Low"),
            ("Active Contributors", f"{len(self.git['contributors'])} total",
             "High" if len(self.git['contributors']) > 20 else "Medium" if len(self.git['contributors']) > 10 else "Low"),
            ("Avg Contributors/Month", f"{self.git['avg_monthly_contributors']:.1f}",
             "High" if self.git['avg_monthly_contributors'] > 10 else "Medium" if self.git['avg_monthly_contributors'] > 5 else "Low"),
        ]

        y_pos = 1.3
        for label, value, health in metrics:
            # Metric box
            box = slide.shapes.add_shape(
                1,  # Rectangle
                Inches(0.5), Inches(y_pos),
                Inches(9), Inches(1)
            )
            box.fill.solid()
            box.fill.fore_color.rgb = COLORS['neutral']
            box.line.color.rgb = COLORS['primary']

            # Label
            label_box = slide.shapes.add_textbox(
                Inches(0.7), Inches(y_pos + 0.15),
                Inches(4), Inches(0.6)
            )
            label_frame = label_box.text_frame
            label_frame.text = label
            label_para = label_frame.paragraphs[0]
            label_para.font.size = Pt(16)
            label_para.font.bold = True
            label_para.font.color.rgb = COLORS['primary']

            # Value
            value_box = slide.shapes.add_textbox(
                Inches(5), Inches(y_pos + 0.15),
                Inches(2.5), Inches(0.6)
            )
            value_frame = value_box.text_frame
            value_frame.text = value
            value_para = value_frame.paragraphs[0]
            value_para.font.size = Pt(16)
            value_para.font.color.rgb = COLORS['dark']

            # Health indicator
            health_box = slide.shapes.add_textbox(
                Inches(7.8), Inches(y_pos + 0.15),
                Inches(1.5), Inches(0.6)
            )
            health_frame = health_box.text_frame
            health_frame.text = health
            health_para = health_frame.paragraphs[0]
            health_para.alignment = PP_ALIGN.CENTER
            health_para.font.size = Pt(16)
            health_para.font.bold = True
            if health == "High":
                health_para.font.color.rgb = COLORS['success']
            elif health == "Medium":
                health_para.font.color.rgb = COLORS['warning']
            else:
                health_para.font.color.rgb = COLORS['accent']

            y_pos += 1.2

    def _add_recommendations_slide(self, prs):
        """Add strategic recommendations slide"""
        slide = prs.slides.add_slide(prs.slide_layouts[6])

        # Title
        title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.3), Inches(9), Inches(0.6))
        title_frame = title_box.text_frame
        title_frame.text = "Strategic Recommendations"
        title_para = title_frame.paragraphs[0]
        title_para.font.size = Pt(32)
        title_para.font.bold = True
        title_para.font.color.rgb = COLORS['primary']

        # Recommendations
        recommendations = [
            "Address High-Priority Issues: Focus on resolving the oldest open issues with high engagement to improve user satisfaction",
            "Improve Issue Triage: Implement better labeling and prioritization for incoming issues to reduce resolution time",
            "Expand Contributor Base: Actively recruit and onboard new contributors to reduce dependency on core team members",
            "Establish Response SLAs: Set target response times for different issue types to improve community engagement",
            "Regular Health Reviews: Schedule monthly reviews of project health metrics to identify trends early",
            "Documentation Focus: Address documentation-related issues to reduce support burden and improve onboarding"
        ]

        y_pos = 1.2
        for i, rec in enumerate(recommendations, 1):
            # Number circle
            circle = slide.shapes.add_shape(
                9,  # Oval
                Inches(0.6), Inches(y_pos),
                Inches(0.4), Inches(0.4)
            )
            circle.fill.solid()
            circle.fill.fore_color.rgb = COLORS['accent']
            circle.line.color.rgb = COLORS['accent']

            # Number
            num_box = slide.shapes.add_textbox(
                Inches(0.6), Inches(y_pos),
                Inches(0.4), Inches(0.4)
            )
            num_frame = num_box.text_frame
            num_frame.text = str(i)
            num_para = num_frame.paragraphs[0]
            num_para.alignment = PP_ALIGN.CENTER
            num_para.font.size = Pt(16)
            num_para.font.bold = True
            num_para.font.color.rgb = RGBColor(255, 255, 255)

            # Recommendation text
            text_box = slide.shapes.add_textbox(
                Inches(1.2), Inches(y_pos - 0.05),
                Inches(8.2), Inches(0.8)
            )
            text_frame = text_box.text_frame
            text_frame.text = rec
            text_frame.word_wrap = True
            text_para = text_frame.paragraphs[0]
            text_para.font.size = Pt(13)
            text_para.font.color.rgb = COLORS['dark']

            y_pos += 0.95

    def create_markdown_report(self):
        """Create detailed Markdown report"""
        print("\nGenerating Markdown report...")

        md_content = []

        # Header
        md_content.append("# Logseq Project Executive Report\n")
        md_content.append(f"**Repository:** {GITHUB_REPO}\n")
        md_content.append(f"**Generated:** {datetime.now().strftime('%B %d, %Y at %H:%M')}\n")
        md_content.append(f"**Analysis Period:** Last {MONTHS_TO_ANALYZE} months for commits, all time for issues\n")
        md_content.append("\n---\n")

        # Executive Summary
        md_content.append("\n## Executive Summary\n")
        md_content.append(f"- **Total Issues:** {self.github['total_issues']}\n")
        md_content.append(f"- **Open Issues:** {self.github['open_issues']}\n")
        md_content.append(f"- **Closed Issues:** {self.github['closed_issues']}\n")
        md_content.append(f"- **Resolution Rate:** {self.github['resolution_rate']:.1f}%\n")
        md_content.append(f"- **Median Time to Close:** {self.github['median_close_time']:.0f} days\n")
        md_content.append(f"- **Total Commits (24mo):** {self.git['total_commits']}\n")
        md_content.append(f"- **Active Contributors:** {len(self.git['contributors'])}\n")
        md_content.append(f"- **Avg Monthly Contributors:** {self.git['avg_monthly_contributors']:.1f}\n")

        # Issue Analysis
        md_content.append("\n## Issue Analysis\n")

        # Issues by label
        md_content.append("\n### Issues by Label\n")
        md_content.append("\n| Label | Total | Open | Closed |\n")
        md_content.append("|-------|-------|------|--------|\n")

        for label in sorted(self.github['by_label'].keys(), key=lambda x: len(self.github['by_label'][x]), reverse=True):
            total = len(self.github['by_label'][label])
            open_count = self.github['by_state_and_label'][label]['open']
            closed_count = self.github['by_state_and_label'][label]['closed']
            md_content.append(f"| {label} | {total} | {open_count} | {closed_count} |\n")

        # Top reporters
        md_content.append("\n### Top Issue Reporters\n")
        md_content.append("\n| Rank | Reporter | Issues |\n")
        md_content.append("|------|----------|--------|\n")

        for i, (reporter, count) in enumerate(self.github['reporters'].most_common(20), 1):
            md_content.append(f"| {i} | {reporter} | {count} |\n")

        # Problem areas
        md_content.append("\n### Top Problem Areas (Oldest Open Issues with High Engagement)\n")

        for i, issue in enumerate(self.github['old_open_issues'][:10], 1):
            md_content.append(f"\n#### {i}. Issue #{issue['number']}: {issue['title']}\n")
            md_content.append(f"- **Age:** {issue['age_days']} days\n")
            md_content.append(f"- **Comments:** {issue['comments']}\n")
            md_content.append(f"- **Labels:** {', '.join(issue['labels'])}\n")
            md_content.append(f"- **URL:** {issue['url']}\n")

        # Git Analysis
        md_content.append("\n## Git Repository Analysis\n")

        # Commit history
        md_content.append("\n### Monthly Commit History\n")
        md_content.append("\n| Month | Commits | Active Contributors |\n")
        md_content.append("|-------|---------|--------------------|\n")

        for month in sorted(self.git['by_month'].keys()):
            commits = self.git['by_month'][month]
            contributors = len(self.git['monthly_contributors'][month])
            md_content.append(f"| {month} | {commits} | {contributors} |\n")

        # Top contributors
        md_content.append("\n### Top Contributors (by commits)\n")
        md_content.append("\n| Rank | Contributor | Commits |\n")
        md_content.append("|------|-------------|--------|\n")

        for i, (name, commits) in enumerate(sorted(self.git['by_author'].items(), key=lambda x: x[1], reverse=True)[:20], 1):
            md_content.append(f"| {i} | {name} | {commits} |\n")

        # Project Health
        md_content.append("\n## Project Health Assessment\n")

        avg_commits = self.git['total_commits'] / MONTHS_TO_ANALYZE if MONTHS_TO_ANALYZE > 0 else 0

        md_content.append(f"\n### Key Health Indicators\n")
        md_content.append(f"- **Issue Resolution Rate:** {self.github['resolution_rate']:.1f}% ")
        md_content.append(f"({'Healthy' if self.github['resolution_rate'] > 70 else 'Needs Improvement' if self.github['resolution_rate'] > 50 else 'Critical'})\n")
        md_content.append(f"- **Monthly Commit Velocity:** {avg_commits:.0f} commits/month\n")
        md_content.append(f"- **Contributor Diversity:** {len(self.git['contributors'])} total contributors\n")
        md_content.append(f"- **Average Monthly Active Contributors:** {self.git['avg_monthly_contributors']:.1f}\n")

        md_content.append("\n### Health Analysis\n")

        if self.github['resolution_rate'] > 70:
            md_content.append("- ✅ **Issue Management:** Strong issue resolution rate indicates effective triage and closure processes\n")
        else:
            md_content.append("- ⚠️ **Issue Management:** Resolution rate suggests backlog growth; consider prioritizing issue closure\n")

        if avg_commits > 100:
            md_content.append("- ✅ **Development Velocity:** High commit frequency indicates active development\n")
        elif avg_commits > 50:
            md_content.append("- ⚙️ **Development Velocity:** Moderate commit activity; room for acceleration\n")
        else:
            md_content.append("- ⚠️ **Development Velocity:** Low commit frequency may indicate resource constraints\n")

        if len(self.git['contributors']) > 20:
            md_content.append("- ✅ **Contributor Base:** Healthy contributor diversity reduces bus factor risk\n")
        else:
            md_content.append("- ⚠️ **Contributor Base:** Limited contributor diversity increases project risk\n")

        # Recommendations
        md_content.append("\n## Strategic Recommendations\n")

        recommendations = [
            {
                'title': 'Address High-Priority Issues',
                'description': 'Focus on resolving the oldest open issues with high engagement to improve user satisfaction',
                'rationale': f'Currently {self.github["open_issues"]} issues are open, with some dating back over {self.github["old_open_issues"][0]["age_days"] if self.github["old_open_issues"] else 0} days'
            },
            {
                'title': 'Improve Issue Triage',
                'description': 'Implement better labeling and prioritization for incoming issues',
                'rationale': f'Median close time of {self.github["median_close_time"]:.0f} days suggests room for process improvement'
            },
            {
                'title': 'Expand Contributor Base',
                'description': 'Actively recruit and onboard new contributors',
                'rationale': f'With {len(self.git["contributors"])} total contributors, expanding the base will reduce dependency risk'
            },
            {
                'title': 'Establish Response SLAs',
                'description': 'Set target response times for different issue types',
                'rationale': 'Consistent response times improve community engagement and user satisfaction'
            },
            {
                'title': 'Regular Health Reviews',
                'description': 'Schedule monthly reviews of project health metrics',
                'rationale': 'Proactive monitoring helps identify trends before they become critical issues'
            },
            {
                'title': 'Documentation Focus',
                'description': 'Address documentation-related issues to reduce support burden',
                'rationale': 'Better documentation reduces issue volume and improves contributor onboarding'
            }
        ]

        for i, rec in enumerate(recommendations, 1):
            md_content.append(f"\n### {i}. {rec['title']}\n")
            md_content.append(f"**Action:** {rec['description']}\n\n")
            md_content.append(f"**Rationale:** {rec['rationale']}\n")

        # Charts
        md_content.append("\n## Visualizations\n")
        md_content.append("\nAll charts are available in the `charts/` subdirectory:\n")
        md_content.append("- `all_issues_by_label.png` - Distribution of all issues by label\n")
        md_content.append("- `open_issues_by_label.png` - Currently open issues by label\n")
        md_content.append("- `issue_status.png` - Pie chart of open vs closed issues\n")
        md_content.append("- `top_reporters.png` - Top 10 issue reporters\n")
        md_content.append("- `commit_trend.png` - Monthly commit activity trend\n")
        md_content.append("- `top_contributors.png` - Top 10 code contributors\n")

        # Appendix: All Issues
        md_content.append("\n## Appendix: Complete Issue List by Label\n")

        for label in sorted(self.github['by_label'].keys()):
            md_content.append(f"\n### Label: {label}\n")
            md_content.append("\n| Number | Title | State | Age (days) | Comments |\n")
            md_content.append("|--------|-------|-------|------------|----------|\n")

            issues = self.github['by_label'][label]
            issues_sorted = sorted(issues, key=lambda x: x['number'])

            for issue in issues_sorted[:50]:  # Limit to first 50 per label
                created = datetime.strptime(issue['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                age = (datetime.now() - created).days
                title = issue['title'][:60] + '...' if len(issue['title']) > 60 else issue['title']
                title = title.replace('|', '\\|')  # Escape pipes in markdown
                md_content.append(f"| #{issue['number']} | {title} | {issue['state']} | {age} | {issue['comments']} |\n")

            if len(issues) > 50:
                md_content.append(f"\n*({len(issues) - 50} more issues with this label...)*\n")

        # Save markdown
        output_path = self.output_dir / 'logseq_executive_report.md'
        with open(output_path, 'w') as f:
            f.write(''.join(md_content))

        print(f"Markdown report saved to: {output_path}")

        return output_path


def main():
    """Main execution function"""
    print("=" * 70)
    print("Logseq Project Executive Report Generator")
    print("=" * 70)

    # Analyze GitHub issues
    github_analyzer = GitHubAnalyzer(GITHUB_REPO)
    github_analyzer.fetch_all_issues()
    github_analysis = github_analyzer.analyze_issues()

    # Analyze Git repository
    git_analyzer = GitAnalyzer(GIT_REPO_PATH, BRANCH)
    commits = git_analyzer.get_commit_history(MONTHS_TO_ANALYZE)
    git_analysis = git_analyzer.analyze_commits(commits)

    # Generate reports
    report_gen = ReportGenerator(github_analysis, git_analysis, OUTPUT_DIR)

    pptx_path = report_gen.create_powerpoint()
    md_path = report_gen.create_markdown_report()

    print("\n" + "=" * 70)
    print("Report generation complete!")
    print("=" * 70)
    print(f"\nPowerPoint: {pptx_path}")
    print(f"Markdown:   {md_path}")
    print(f"Charts:     {report_gen.charts_dir}/")
    print()


if __name__ == "__main__":
    main()
