(() => {
  const diffCode = document.querySelector('code.language-diff');
  if (!diffCode) {
    return;
  }
  const copyDiffButton = document.getElementById('copy-diff-btn');
  const codeArea = diffCode.closest('.code-area');

  const setButtonLabelTemporarily = (button, successLabel, defaultLabel) => {
    if (!button) {
      return;
    }
    button.textContent = successLabel;
    window.setTimeout(() => {
      button.textContent = defaultLabel;
    }, 1200);
  };

  if (copyDiffButton) {
    copyDiffButton.addEventListener('click', async () => {
      const text = diffCode.textContent || '';
      try {
        if (navigator.clipboard && navigator.clipboard.writeText) {
          await navigator.clipboard.writeText(text);
        } else {
          const fallback = document.createElement('textarea');
          fallback.value = text;
          fallback.setAttribute('readonly', '');
          fallback.style.position = 'absolute';
          fallback.style.left = '-9999px';
          document.body.appendChild(fallback);
          fallback.select();
          document.execCommand('copy');
          document.body.removeChild(fallback);
        }
        setButtonLabelTemporarily(copyDiffButton, 'Copied', 'Copy Patch');
      } catch (_error) {
        setButtonLabelTemporarily(copyDiffButton, 'Copy Failed', 'Copy Patch');
      }
    });

    const updateCopyButtonPlacement = () => {
      if (!codeArea) {
        return;
      }

      const areaRect = codeArea.getBoundingClientRect();
      const shouldFix = areaRect.top < 0 && areaRect.bottom > 0;
      copyDiffButton.classList.toggle('is-fixed', shouldFix);

      if (shouldFix) {
        const rootStyles = window.getComputedStyle(document.documentElement);
        const spacing = Number.parseFloat(rootStyles.getPropertyValue('--space-4')) || 16;
        const fixedLeft = Math.max(10, areaRect.right - copyDiffButton.offsetWidth - spacing);
        copyDiffButton.style.left = `${fixedLeft}px`;
        copyDiffButton.style.right = 'auto';
      } else {
        copyDiffButton.style.left = '';
        copyDiffButton.style.right = '';
      }
    };

    updateCopyButtonPlacement();
    window.addEventListener('scroll', updateCopyButtonPlacement, { passive: true });
    window.addEventListener('resize', updateCopyButtonPlacement);
  }

  const classifyLine = (line) => {
    if (line.startsWith('+++ ') || line.startsWith('--- ')) {
      return 'diff-file';
    }
    if (line.startsWith('@@')) {
      return 'diff-hunk';
    }
    if (line.startsWith('+')) {
      return 'diff-add';
    }
    if (line.startsWith('-')) {
      return 'diff-del';
    }
    if (line.startsWith('diff ') || line.startsWith('index ') || line.startsWith('\\')) {
      return 'diff-meta';
    }
    return '';
  };

  const lines = (diffCode.textContent || '').split('\n');
  const fragment = document.createDocumentFragment();

  lines.forEach((line, index) => {
    const span = document.createElement('span');
    const tokenClass = classifyLine(line);
    if (tokenClass) {
      span.className = `token ${tokenClass}`;
    }
    span.textContent = line;
    fragment.appendChild(span);

    if (index < lines.length - 1) {
      fragment.appendChild(document.createTextNode('\n'));
    }
  });

  diffCode.replaceChildren(fragment);
})();
