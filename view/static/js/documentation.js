// JavaScript para manejar la lógica del acordeón
const accordionButtons = document.querySelectorAll('.accordion__button');

accordionButtons.forEach(button => {
    button.addEventListener('click', () => {
        const content = button.nextElementSibling;
        content.style.display = content.style.display === 'block' ? 'none' : 'block';
    });
});