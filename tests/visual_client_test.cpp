#include "include/dgs/network.h"
#include "include/dgs/packet.h"

#include <SFML/Graphics.hpp>
#include <iostream>

int main() {
    sf::RenderWindow window(sf::VideoMode(800, 600), "DGS Engine Stress Test");
    
    // Configuración inicial
    float currentSpeed = 200.0f; // Unidades por segundo
    DGS::EntityTransfer entity{};
    entity.uuid = 1001;
    entity.stats.speed[0] = currentSpeed;

    // 1. Conexión inicial al HeadServer
    DGS::TCPSocket head;
    if(!head.connect("127.0.0.1", 42424)) return -1;

    // 2. Simulación de bucle
    sf::Clock deltaClock;
    sf::CircleShape visualPlayer(10.f);
    visualPlayer.setFillColor(sf::Color::Cyan);

    while (window.isOpen()) {
        float dt = deltaClock.restart().asSeconds();
        sf::Event event;
        while (window.pollEvent(event)) {
            if (event.type == sf::Event::Closed) window.close();
        }

        // --- LÓGICA DE TEST (CHEATS) ---
        float speedMultiplier = 1.0f;
        if (sf::Keyboard::isKeyPressed(sf::Keyboard::E)) {
            speedMultiplier = 10.0f; // SPEED HACK ACTIVADO
            visualPlayer.setFillColor(sf::Color::Red);
        } else {
            visualPlayer.setFillColor(sf::Color::Cyan);
        }

        // Movimiento
        sf::Vector2f mov(0, 0);
        if (sf::Keyboard::isKeyPressed(sf::Keyboard::W)) mov.y -= 1;
        if (sf::Keyboard::isKeyPressed(sf::Keyboard::S)) mov.y += 1;
        if (sf::Keyboard::isKeyPressed(sf::Keyboard::A)) mov.x -= 1;
        if (sf::Keyboard::isKeyPressed(sf::Keyboard::D)) mov.x += 1;

        visualPlayer.move(mov * currentSpeed * speedMultiplier * dt);

        // --- ENVIAR AL MOTOR PARA VALIDACIÓN ---
        // Preparamos el paquete de la entidad
        entity.pos[0] = visualPlayer.getPosition().x;
        entity.pos[1] = visualPlayer.getPosition().y;
        
        DGS::Packet pkg;
        pkg.pack(entity);
        
        // Enviamos al AntiCheat (asumiendo que tienes el port-forward)
        // head.send(head.getSocketFD(), pkg.getRawData(), pkg.getSize());

        window.clear();
        window.draw(visualPlayer);
        window.display();
    }
    return 0;
}