# Contributing to Incrementally Refresh Dataflow

Thank you for your interest in contributing to this project! We welcome contributions from the community to help improve this Microsoft Fabric incremental refresh framework.

## How to Contribute

### Reporting Issues

If you encounter a bug or have a feature request:

1. **Search existing issues** to see if your issue has already been reported
2. **Create a new issue** with a clear title and description
3. **Include relevant details**:
   - Steps to reproduce the issue
   - Expected vs. actual behavior
   - Microsoft Fabric environment details (capacity tier, region)
   - Notebook version
   - Error messages or logs
   - Screenshots if applicable

### Suggesting Enhancements

We welcome suggestions for new features or improvements:

1. **Open an issue** with the `enhancement` label
2. **Describe the enhancement** in detail:
   - What problem does it solve?
   - How would it work?
   - Potential implementation approach
   - Any alternatives you've considered

### Contributing Code

#### Getting Started

1. **Fork the repository** to your GitHub account
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR-USERNAME/refresh-dataflow.git
   cd refresh-dataflow
   ```
3. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

#### Making Changes

1. **Make your changes** to the notebook or documentation
2. **Test thoroughly**:
   - Test with both CI/CD and regular dataflows
   - Verify bucket processing and retry logic
   - Test initial load and incremental refresh scenarios
   - Confirm tracking table updates work correctly
3. **Follow coding standards**:
   - Use clear, descriptive variable names
   - Add comments for complex logic
   - Keep functions focused and modular
   - Follow Python PEP 8 style guidelines

#### Submitting Changes

1. **Commit your changes** with a clear commit message:
   ```bash
   git add .
   git commit -m "Add feature: brief description of your changes"
   ```
2. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```
3. **Open a Pull Request**:
   - Go to the original repository
   - Click "New Pull Request"
   - Select your feature branch
   - Fill out the PR template with:
     - Description of changes
     - Related issue number (if applicable)
     - Testing performed
     - Screenshots (if relevant)

### Pull Request Guidelines

- **Keep PRs focused**: One feature or fix per PR
- **Write clear descriptions**: Explain what and why, not just how
- **Reference issues**: Link to related issues using `Fixes #123` or `Related to #456`
- **Be responsive**: Address review comments promptly
- **Update documentation**: Update README or other docs if your changes affect usage

## Development Guidelines

### Notebook Structure

When modifying the notebook:

- **Keep cells organized**: Maintain logical grouping of functionality
- **Preserve constants section**: Don't change the structure of configuration constants
- **Document parameters**: Update parameter documentation if adding new parameters
- **Error handling**: Ensure proper error messages and logging

### Testing Checklist

Before submitting changes, verify:

- [ ] Works with regular Dataflow Gen2
- [ ] Works with CI/CD Dataflow Gen2
- [ ] Initial load scenario functions correctly
- [ ] Incremental refresh scenario functions correctly
- [ ] Failed bucket retry logic works
- [ ] Tracking table updates correctly
- [ ] Pipeline failure integration works (sys.exit codes)
- [ ] Documentation is updated
- [ ] No breaking changes to existing functionality

## Code of Conduct

Please note that this project adheres to a Code of Conduct. By participating, you are expected to uphold this code. Please review [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for details.

## Questions?

If you have questions about contributing:

- Open an issue with the `question` label
- Review existing issues and documentation
- Check the [Troubleshooting](../readme.md#troubleshooting) section in the README

## License

By contributing to this project, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to make this Microsoft Fabric incremental refresh framework better for everyone!
