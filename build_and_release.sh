#!/bin/bash

# Build and Release Script for shift_left CLI
# This script automates the release branch creation and build process
# based on the instructions in contributing.md

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
usage() {
    echo "Usage: $0 <version>"
    echo "Example: $0 0.1.34"
    echo ""
    echo "This script will:"
    echo "  1. Create a release branch from develop"
    echo "  2. Update version in cli.py and pyproject.toml"
    echo "  3. Build the wheel package"
    echo "  4. Clean old wheels (keep last 10)"
    echo "  5. Update changelog with recent commits"
    echo "  6. Commit the changes"
    echo ""
    echo "Note: You'll need to manually merge to main, tag, and merge back to develop"
    exit 1
}

# Check if version is provided
if [ $# -eq 0 ]; then
    print_error "Version number is required"
    usage
fi

VERSION=$1
RELEASE_BRANCH="v${VERSION}"

# Validate version format (basic check)
if [[ ! $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    print_error "Invalid version format. Expected format: x.y.z (e.g., 0.1.34)"
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "src/shift_left/pyproject.toml" ]; then
    print_error "Please run this script from the shift_left_utils root directory"
    exit 1
fi

# Check if we have uv installed
if ! command -v uv &> /dev/null; then
    print_error "uv is not installed. Please install uv first: https://docs.astral.sh/uv/getting-started/installation/"
    exit 1
fi

print_info "Starting release process for version ${VERSION}"
# Step 0: Check if the current branch ais develop
if [ $(git branch --show-current) != "develop" ]; then
    print_error "Please switch to develop branch and be sure it is up to date with your last feature branches"
    exit 1
fi

# Step 1: Create release branch from develop
print_info "Creating release branch ${RELEASE_BRANCH} from develop"

# Ensure we're on develop and it's up to date
git checkout develop
git pull origin develop

# Create release branch
git checkout -b ${RELEASE_BRANCH} develop
print_success "Created release branch ${RELEASE_BRANCH}"

# Step 2: Update version in app_config.py
print_info "Updating version in app_config.py"
APP_CONFIG_FILE="src/shift_left/shift_left/core/utils/app_config.py"
sed -i.bak "s/__version__ = \".*\"/__version__ = \"${VERSION}\"/" ${APP_CONFIG_FILE}
rm ${APP_CONFIG_FILE}.bak
print_success "Updated app_config.py version to ${VERSION}"

# Step 3: Update version in pyproject.toml
print_info "Updating version in pyproject.toml"
PYPROJECT_FILE="src/shift_left/pyproject.toml"
sed -i.bak "s/version = \".*\"/version = \"${VERSION}\"/" ${PYPROJECT_FILE}
rm ${PYPROJECT_FILE}.bak
print_success "Updated pyproject.toml version to ${VERSION}"

# Step 4: Build the wheel
print_info "Building the wheel package"
cd src/shift_left

# Build the package
uv build .
print_success "Built wheel package successfully"

# Remove .gitignore in dist if it exists
if [ -f "dist/.gitignore" ]; then
    print_info "Removing .gitignore from dist folder"
    rm dist/.gitignore
fi

# Step 5: Clean old wheels (keep last 10)
print_info "Cleaning old wheel files (keeping last 10)"
cd dist

# Count wheel files and remove older ones if more than 10
WHEEL_COUNT=$(ls -1 *.whl 2>/dev/null | wc -l)
if [ ${WHEEL_COUNT} -gt 10 ]; then
    print_info "Found ${WHEEL_COUNT} wheel files, removing older ones"
    ls -1t *.whl | tail -n +11 | xargs rm -f
    print_success "Cleaned old wheel files"
else
    print_info "Found ${WHEEL_COUNT} wheel files, no cleanup needed"
fi

# Do the same for tar.gz files
TAR_COUNT=$(ls -1 *.tar.gz 2>/dev/null | wc -l)
if [ ${TAR_COUNT} -gt 10 ]; then
    print_info "Found ${TAR_COUNT} tar.gz files, removing older ones"
    ls -1t *.tar.gz | tail -n +11 | xargs rm -f
    print_success "Cleaned old tar.gz files"
else
    print_info "Found ${TAR_COUNT} tar.gz files, no cleanup needed"
fi

cd ../..

# Step 6: Update changelog
print_info "Updating CHANGELOG.md with recent commits"
CHANGELOG_FILE="./CHANGELOG.md"

# Get recent commits between main and develop
RECENT_COMMITS=$(git log --oneline main..develop --reverse)

if [ ! -z "$RECENT_COMMITS" ]; then
    # Create a temporary file with new changelog entry
    TEMP_CHANGELOG=$(mktemp)

    # Add new version header
    echo "## [${VERSION}] - $(date +%Y-%m-%d)" > ${TEMP_CHANGELOG}
    echo "" >> ${TEMP_CHANGELOG}
    echo "### Changes:" >> ${TEMP_CHANGELOG}

    # Add recent commits
    while IFS= read -r commit; do
        echo "- ${commit}" >> ${TEMP_CHANGELOG}
    done <<< "${RECENT_COMMITS}"

    echo "" >> ${TEMP_CHANGELOG}

    # Prepend to existing changelog if it exists
    if [ -f ${CHANGELOG_FILE} ]; then
        cat ${CHANGELOG_FILE} >> ${TEMP_CHANGELOG}
    fi

    mv ${TEMP_CHANGELOG} ${CHANGELOG_FILE}
    print_success "Updated CHANGELOG.md with recent commits"
else
    print_warning "No new commits found between main and develop"
fi


# Step 7: Commit the changes
print_info "Committing version update changes"
git add .
git commit -m "Release version ${VERSION}

- Update version in cli.py and pyproject.toml
- Build wheel package
- Update CHANGELOG.md
- Clean old wheel files"

print_success "Committed release changes"

# Final instructions
echo ""
print_success "Release branch ${RELEASE_BRANCH} created and prepared successfully!"
echo ""
print_info "Next steps (manual):"
echo "1. Run tests and make any necessary fixes on this release branch"
echo "2. When ready, merge to main:"
echo "   ${YELLOW}git checkout main${NC}"
echo "   ${YELLOW}git merge --no-ff ${RELEASE_BRANCH}${NC}"
echo "   ${YELLOW}git push${NC}"
echo ""
echo "3. Tag the main branch:"
echo "   ${YELLOW}git tag -a ${VERSION} -m \"Release version ${VERSION}\"${NC}"
echo "   ${YELLOW}git push origin ${VERSION}${NC}"
echo ""
echo "4. Merge back to develop:"
echo "   ${YELLOW}git checkout develop${NC}"
echo "   ${YELLOW}git merge --no-ff main ${NC}"
echo "   ${YELLOW}git push${NC}"
echo ""
echo "5. Create GitHub release referencing tag ${VERSION}"
echo ""
echo "6. Delete release branch:"
echo "   ${YELLOW}git branch -d ${RELEASE_BRANCH}${NC}"
echo ""
echo "7. Update the mkdocs.yml file with the new version"
echo "   ${YELLOW}sed -i 's/site_name: Shift Left Utilities v.*$/site_name: Shift Left Utilities v${VERSION}/' mkdocs.yml${NC}"
echo "   ${YELLOW}git commit -m \"Update mkdocs.yml to version ${VERSION}\"${NC}"
echo "   ${YELLOW}git push${NC}"
echo ""
echo "8. Update push new pypi package"
echo "   export UV_PUBLISH_TOKEN=YOUR_TOKEN_HERE"
echo "   ${YELLOW}uv publish${NC}"
echo ""
echo "Files updated:"
echo "- ${CLI_FILE}"
echo "- ${PYPROJECT_FILE}"
echo "- ${CHANGELOG_FILE}"
echo "- Built wheel: src/shift_left/dist/shift_left-${VERSION}-py3-none-any.whl"
