function Canvas() {
  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;

    console.log(x, y);
  };

  return (
    <svg width="100%" height="100%" onMouseMove={handleMouseMove}>
      <rect x="0" y="0" width="100%" height="100%" fill="white" />
    </svg>
  );
}

export default Canvas;
